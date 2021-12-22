import abc
from typing import Iterable

from dagster.core.definitions.run_request import InstigatorType
from dagster.core.instance import MayHaveInstanceWeakref
from dagster.core.scheduler.instigation import InstigatorState, InstigatorTick, TickData, TickStatus


class ScheduleStorage(abc.ABC, MayHaveInstanceWeakref):
    """Abstract class for managing persistance of scheduler artifacts"""

    @abc.abstractmethod
    def wipe(self):
        """Delete all schedules from storage"""

    @abc.abstractmethod
    def all_stored_job_state(
        self, repository_origin_id: str = None, job_type: InstigatorType = None
    ) -> Iterable[InstigatorState]:
        """Return all InstigationStates present in storage

        Args:
            repository_origin_id (Optional[str]): The ExternalRepository target id to scope results to
            job_type (Optional[InstigatorType]): The InstigatorType to scope results to
        """

    @abc.abstractmethod
    def get_job_state(self, job_origin_id: str) -> InstigatorState:
        """Return the unique job with the given id

        Args:
            job_origin_id (str): The unique job identifier
        """

    @abc.abstractmethod
    def add_job_state(self, job: InstigatorState):
        """Add a job to storage.

        Args:
            job (InstigatorState): The job to add
        """

    @abc.abstractmethod
    def update_job_state(self, job: InstigatorState):
        """Update a job in storage.

        Args:
            job (InstigatorState): The job to update
        """

    @abc.abstractmethod
    def delete_job_state(self, job_origin_id: str):
        """Delete a job in storage.

        Args:
            job_origin_id (str): The id of the ExternalJob target to delete
        """

    @abc.abstractmethod
    def get_job_ticks(
        self, job_origin_id: str, before: float = None, after: float = None, limit: int = None
    ) -> Iterable[InstigatorTick]:
        """Get the ticks for a given job.

        Args:
            job_origin_id (str): The id of the ExternalJob target
        """

    @abc.abstractmethod
    def get_latest_job_tick(self, job_origin_id: str) -> InstigatorTick:
        """Get the most recent tick for a given job.

        Args:
            job_origin_id (str): The id of the ExternalJob target
        """

    @abc.abstractmethod
    def create_job_tick(self, job_tick_data: TickData):
        """Add a job tick to storage.

        Args:
            job_tick_data (TickData): The job tick to add
        """

    @abc.abstractmethod
    def update_job_tick(self, tick: InstigatorTick):
        """Update a job tick already in storage.

        Args:
            tick (InstigatorTick): The job tick to update
        """

    @abc.abstractmethod
    def purge_job_ticks(self, job_origin_id: str, tick_status: TickStatus, before: float):
        """Wipe ticks for a job for a certain status and timestamp.

        Args:
            job_origin_id (str): The id of the ExternalJob target to delete
            tick_status (TickStatus): The tick status to wipe
            before (datetime): All ticks before this datetime will get purged
        """

    @abc.abstractmethod
    def get_job_tick_stats(self, job_origin_id: str):
        """Get tick stats for a given job.

        Args:
            job_origin_id (str): The id of the ExternalJob target
        """

    @abc.abstractmethod
    def upgrade(self):
        """Perform any needed migrations"""

    def optimize_for_dagit(self, statement_timeout: int):
        """Allows for optimizing database connection / use in the context of a long lived dagit process"""
