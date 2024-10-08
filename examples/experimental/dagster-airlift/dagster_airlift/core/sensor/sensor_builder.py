from datetime import timedelta
from typing import Callable, Iterable, Iterator, List, Mapping, Optional, Sequence, Set, Union

from dagster import (
    AssetCheckKey,
    AssetKey,
    AssetMaterialization,
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    _check as check,
    sensor,
)
from dagster._core.definitions.asset_check_evaluation import AssetCheckEvaluation
from dagster._core.definitions.asset_selection import AssetSelection
from dagster._core.definitions.definitions_class import Definitions
from dagster._core.definitions.events import AssetObservation
from dagster._core.definitions.repository_definition.repository_definition import (
    RepositoryDefinition,
)
from dagster._core.errors import (
    DagsterInvariantViolationError,
    DagsterUserCodeExecutionError,
    user_code_error_boundary,
)
from dagster._core.storage.dagster_run import DagsterRun, RunsFilter
from dagster._grpc.client import DEFAULT_SENSOR_GRPC_TIMEOUT
from dagster._record import record
from dagster._serdes import deserialize_value, serialize_value
from dagster._serdes.serdes import whitelist_for_serdes
from dagster._time import datetime_from_timestamp, get_current_datetime

from dagster_airlift.constants import (
    DAG_ID_TAG_KEY,
    DAG_RUN_ID_TAG_KEY,
    EFFECTIVE_TIMESTAMP_METADATA_KEY,
    TASK_ID_TAG_KEY,
)
from dagster_airlift.core.airflow_defs_data import AirflowDefinitionsData
from dagster_airlift.core.airflow_instance import AirflowInstance, DagRun, TaskInstance
from dagster_airlift.core.serialization.serialized_data import DagHandle, TaskHandle

MAIN_LOOP_TIMEOUT_SECONDS = DEFAULT_SENSOR_GRPC_TIMEOUT - 20
DEFAULT_AIRFLOW_SENSOR_INTERVAL_SECONDS = 1
START_LOOKBACK_SECONDS = 60  # Lookback one minute in time for the initial setting of the cursor.

AssetEvent = Union[AssetMaterialization, AssetObservation, AssetCheckEvaluation]
DagsterEventTransformerFn = Callable[
    [SensorEvaluationContext, AirflowDefinitionsData, Sequence[AssetMaterialization]],
    Iterable[AssetEvent],
]


@whitelist_for_serdes
@record
class AirflowPollingSensorCursor:
    """A cursor that stores the last effective timestamp and the last polled dag id."""

    end_date_gte: Optional[float] = None
    end_date_lte: Optional[float] = None
    dag_query_offset: Optional[int] = None


class AirliftSensorEventTransformerError(DagsterUserCodeExecutionError):
    """Error raised when an error occurs in the event transformer function."""


def check_keys_for_asset_keys(
    repository_def: RepositoryDefinition, asset_keys: Set[AssetKey]
) -> Iterable[AssetCheckKey]:
    for assets_def in repository_def.asset_graph.assets_defs:
        for check_spec in assets_def.check_specs:
            if check_spec.asset_key in asset_keys:
                yield check_spec.key


def build_airflow_polling_sensor_defs(
    *,
    mapped_defs: Definitions,
    airflow_instance: AirflowInstance,
    event_transformer_fn: Optional[DagsterEventTransformerFn],
    minimum_interval_seconds: int = DEFAULT_AIRFLOW_SENSOR_INTERVAL_SECONDS,
) -> Definitions:
    """The constructed sensor polls the Airflow instance for activity, and inserts asset events into Dagster's event log.

    The sensor decides which Airflow dags and tasks to monitor by inspecting the metadata of the passed-in Definitions object `mapped_defs`.
    The metadata performing this mapping is typically set by calls to `dag_defs` and `task_defs`.

    Using the `event_transformer_fn` argument, users can provide a function that transforms the materializations emitted by the sensor.
    The expected return type of this function is an iterable of `AssetMaterialization`, `AssetObservation`, or `AssetCheckEvaluation` objects.
    Each object is expected to have a metadata key `dagster_airlift.constants.EFFECTIVE_TIMESTAMP_METADATA_KEY` which is a `dagster.TimestampMetadataValue` set.
    This allows Dagster to correctly order the materializations in the event stream.

    Args:
        mapped_defs (Definitions): The `Definitions` object containing assets with metadata mapping them to Airflow dags and tasks.
        airflow_instance (AirflowInstance): The Airflow instance to poll for dag runs.
        event_transformer_fn (Optional[DagsterEventTransformerFn]): A function that transforms the materializations emitted by the sensor.
        minimum_interval_seconds (int): The minimum interval in seconds between sensor runs. Defaults to 1.

    Returns:
        Definitions: A `Definitions` object containing the constructed sensor.
    """
    airflow_data = AirflowDefinitionsData(
        airflow_instance=airflow_instance, mapped_defs=mapped_defs
    )

    @sensor(
        name=f"{airflow_data.airflow_instance.name}__airflow_dag_status_sensor",
        minimum_interval_seconds=minimum_interval_seconds,
        default_status=DefaultSensorStatus.RUNNING,
        # This sensor will only ever execute asset checks and not asset materializations.
        asset_selection=AssetSelection.all_asset_checks(),
    )
    def airflow_dag_sensor(context: SensorEvaluationContext) -> SensorResult:
        """Sensor to report materialization events for each asset as new runs come in."""
        context.log.info(
            f"************Running sensor for {airflow_data.airflow_instance.name}***********"
        )
        try:
            cursor = (
                deserialize_value(context.cursor, AirflowPollingSensorCursor)
                if context.cursor
                else AirflowPollingSensorCursor()
            )
        except Exception as e:
            context.log.info(f"Failed to interpret cursor. Starting from scratch. Error: {e}")
            cursor = AirflowPollingSensorCursor()
        current_date = get_current_datetime()
        current_dag_offset = cursor.dag_query_offset or 0
        end_date_gte = (
            cursor.end_date_gte
            or (current_date - timedelta(seconds=START_LOOKBACK_SECONDS)).timestamp()
        )
        end_date_lte = cursor.end_date_lte or current_date.timestamp()
        sensor_iter = materializations_and_requests_from_batch_iter(
            context=context,
            end_date_gte=end_date_gte,
            end_date_lte=end_date_lte,
            offset=current_dag_offset,
            airflow_data=airflow_data,
        )
        all_asset_events: List[AssetMaterialization] = []
        all_check_keys: Set[AssetCheckKey] = set()
        latest_offset = current_dag_offset
        repository_def = check.not_none(context.repository_def)
        while get_current_datetime() - current_date < timedelta(seconds=MAIN_LOOP_TIMEOUT_SECONDS):
            batch_result = next(sensor_iter, None)
            if batch_result is None:
                break
            all_asset_events.extend(batch_result.asset_events)

            all_check_keys.update(
                check_keys_for_asset_keys(repository_def, batch_result.all_asset_keys_materialized)
            )
            latest_offset = batch_result.idx

        if batch_result is not None:
            new_cursor = AirflowPollingSensorCursor(
                end_date_gte=end_date_gte,
                end_date_lte=end_date_lte,
                dag_query_offset=latest_offset + 1,
            )
        else:
            # We have completed iteration for this range
            new_cursor = AirflowPollingSensorCursor(
                end_date_gte=end_date_lte,
                end_date_lte=None,
                dag_query_offset=0,
            )
        updated_asset_events = _get_transformer_result(
            event_transformer_fn=event_transformer_fn,
            context=context,
            airflow_data=airflow_data,
            all_asset_events=all_asset_events,
        )

        context.update_cursor(serialize_value(new_cursor))

        context.log.info(
            f"************Exiting sensor for {airflow_data.airflow_instance.name}***********"
        )
        return SensorResult(
            asset_events=sorted_asset_events(updated_asset_events, repository_def),
            run_requests=[RunRequest(asset_check_keys=list(all_check_keys))]
            if all_check_keys
            else None,
        )

    return Definitions(sensors=[airflow_dag_sensor])


def sorted_asset_events(
    asset_events: Sequence[AssetEvent],
    repository_def: RepositoryDefinition,
) -> List[AssetEvent]:
    """Sort materializations by end date and toposort order."""
    topo_aks = repository_def.asset_graph.toposorted_asset_keys
    materializations_and_timestamps = [
        (get_timestamp_from_materialization(mat), mat) for mat in asset_events
    ]
    return [
        sorted_event[1]
        for sorted_event in sorted(
            materializations_and_timestamps, key=lambda x: (x[0], topo_aks.index(x[1].asset_key))
        )
    ]


def _get_transformer_result(
    event_transformer_fn: Optional[DagsterEventTransformerFn],
    context: SensorEvaluationContext,
    airflow_data: AirflowDefinitionsData,
    all_asset_events: Sequence[AssetMaterialization],
) -> Sequence[AssetEvent]:
    if not event_transformer_fn:
        return all_asset_events

    with user_code_error_boundary(
        AirliftSensorEventTransformerError,
        lambda: f"Error occurred during event transformation for {airflow_data.airflow_instance.name}",
    ):
        updated_asset_events = list(event_transformer_fn(context, airflow_data, all_asset_events))

    for asset_event in updated_asset_events:
        if not isinstance(
            asset_event, (AssetMaterialization, AssetObservation, AssetCheckEvaluation)
        ):
            raise DagsterInvariantViolationError(
                f"Event transformer function must return AssetMaterialization, AssetObservation, or AssetCheckEvaluation objects. Got {type(asset_event)}."
            )
        if EFFECTIVE_TIMESTAMP_METADATA_KEY not in asset_event.metadata:
            raise DagsterInvariantViolationError(
                f"All returned events must have an effective timestamp, but {asset_event} does not. An effective timestamp can be used by setting dagster_airlift.constants.EFFECTIVE_TIMESTAMP_METADATA_KEY with a dagster.TimestampMetadataValue."
            )
    return updated_asset_events


@record
class BatchResult:
    idx: int
    asset_events: Sequence[AssetMaterialization]
    all_asset_keys_materialized: Set[AssetKey]


def materializations_and_requests_from_batch_iter(
    context: SensorEvaluationContext,
    end_date_gte: float,
    end_date_lte: float,
    offset: int,
    airflow_data: AirflowDefinitionsData,
) -> Iterator[Optional[BatchResult]]:
    runs = airflow_data.airflow_instance.get_dag_runs_batch(
        dag_ids=list(airflow_data.dag_ids_with_mapped_asset_keys),
        end_date_gte=datetime_from_timestamp(end_date_gte),
        end_date_lte=datetime_from_timestamp(end_date_lte),
        offset=offset,
    )
    context.log.info(f"Found {len(runs)} dag runs for {airflow_data.airflow_instance.name}")
    context.log.info(f"All runs {runs}")
    for i, dag_run in enumerate(runs):
        mats = build_synthetic_asset_materializations(
            context, airflow_data.airflow_instance, dag_run, airflow_data
        )
        context.log.info(f"Found {len(mats)} materializations for {dag_run.run_id}")

        all_asset_keys_materialized = {mat.asset_key for mat in mats}
        yield (
            BatchResult(
                idx=i + offset,
                asset_events=mats,
                all_asset_keys_materialized=all_asset_keys_materialized,
            )
            if mats
            else None
        )


def build_synthetic_asset_materializations(
    context: SensorEvaluationContext,
    airflow_instance: AirflowInstance,
    dag_run: DagRun,
    airflow_data: AirflowDefinitionsData,
) -> List[AssetMaterialization]:
    """In this function we need to return the asset materializations we want to synthesize
    on behalf of the user.

    This happens when the user has modeled an external asset that has a corresponding
    task in airflow which is not proxied to Dagster. We want to detect the case
    where there is a successful airflow task instance that is mapped to a
    dagster asset but is _not_ proxied, and then synthensize a materialization
    for observability.

    We do this by querying for successful task instances in Airflow. And then
    for each successful task we see it there exists a Dagster Run tagged with
    the run id. If there is not Dagster run, we know the task was not proxied.

    Task instances are mutable in Airflow, so we are not guaranteed to register
    every task instance. If, for example, the sensor is paused, and then there are
    multiple task clearings, we will only register the last materialization.

    This also currently does not support dynamic tasks in Airflow, in which case
    the use should instead map at the dag-level granularity.
    """
    task_instances = airflow_instance.get_task_instance_batch(
        dag_run=dag_run,
        task_ids=[task_id for task_id in airflow_data.task_ids_in_dag(dag_run.dag_id)],
        states=["success"],
    )

    context.log.info(f"Found {len(task_instances)} task instances for {dag_run.run_id}")
    context.log.info(f"All task instances {task_instances}")

    check.invariant(
        len({ti.task_id for ti in task_instances}) == len(task_instances),
        "Assuming one task instance per task_id for now. Dynamic Airflow tasks not supported.",
    )

    synthetic_mats = []
    runs_by_handle = dagster_runs_by_handle(context, dag_run.run_id)
    for airflow_run_object in [dag_run] + task_instances:
        # If there is no dagster run for this task, we infer that execution was not proxied.
        is_execution_proxied = airflow_run_object.handle in runs_by_handle
        asset_keys_to_emit = (
            airflow_data.mapped_asset_keys(airflow_run_object.handle)
            if not is_execution_proxied
            else airflow_data.autogenerated_asset_keys(airflow_run_object.handle)
        )
        synthetic_mats.extend(
            mat_for_key(airflow_run_object, asset_key) for asset_key in asset_keys_to_emit
        )

        context.log.info(
            f"Dagster run found for task {airflow_run_object.handle} in dag {dag_run.dag_id}. Run {airflow_run_object.run_id}"
        )

    return synthetic_mats


def dagster_runs_by_handle(
    context: SensorEvaluationContext, dag_run_id: str
) -> Mapping[Union[TaskHandle, DagHandle], DagsterRun]:
    # https://linear.app/dagster-labs/issue/FOU-444/make-sensor-work-with-an-airflow-dag-run-that-has-more-than-1000
    dagster_runs = context.instance.get_runs(
        filters=RunsFilter(tags={DAG_RUN_ID_TAG_KEY: dag_run_id}),
        limit=1000,
    )
    context.log.info(
        f"Airlift Sensor: Found dagster run ids: {[run.run_id for run in dagster_runs]}"
        f" for airflow run id {dag_run_id}"
    )
    return {_handle_for_run(run): run for run in dagster_runs}


def _handle_for_run(run: DagsterRun) -> Union[TaskHandle, DagHandle]:
    dag_id = run.tags[DAG_ID_TAG_KEY]
    task_id = run.tags.get(TASK_ID_TAG_KEY)
    return TaskHandle(dag_id, task_id) if task_id else DagHandle(dag_id)


def get_timestamp_from_materialization(event: AssetEvent) -> float:
    return check.float_param(
        event.metadata[EFFECTIVE_TIMESTAMP_METADATA_KEY].value,
        "Materialization Effective Timestamp",
    )


def mat_for_key(
    airflow_run_object: Union[DagRun, TaskInstance],
    asset_key: AssetKey,
) -> AssetMaterialization:
    return AssetMaterialization(
        asset_key=asset_key,
        description=airflow_run_object.note,
        metadata=airflow_run_object.dagster_metadata,
    )
