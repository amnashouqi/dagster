Airlift (dagster-airlift)
=========================


.. currentmodule:: dagster_airlift.core

Core (dagster_airlift.core)
---------------------------

AirflowInstance
^^^^^^^^^^^^^^^^^

.. autoclass:: AirflowInstance

.. autoclass:: AirflowAuthBackend

.. autoclass:: BasicAirflowAuthBackend

Assets & Definitions
^^^^^^^^^^^^^^^^^^^^

.. autofunction:: build_defs_from_airflow_instance

Mapping Dagster assets to Airflow tasks/dags:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: assets_with_task_mappings

.. autofunction:: assets_with_dag_mappings

.. autofunction:: targeted_by_multiple_tasks

Annotations for customizable components:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: DagSelectorFn

.. autoclass:: DagsterEventTransformerFn

.. autoclass:: TaskHandleDict

Objects for retrieving information about the Airflow/Dagster mapping:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: DagInfo

.. autoclass:: AirflowDefinitionsData

.. currentmodule:: dagster_airlift.mwaa

MWAA (dagster_airlift.mwaa)
---------------------------

.. autoclass:: MwaaSessionAuthBackend




