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


MWAA (dagster_airlift.mwaa)
---------------------------
.. currentmodule:: dagster_airlift.mwaa

.. autoclass:: MwaaSessionAuthBackend

In Airflow (dagster_airlift.in_airflow)
---------------------------------------

.. currentmodule:: dagster_airlift.in_airflow

Proxying Function
~~~~~~~~~~~~~~~~~

.. autofunction:: proxying_to_dagster 

Proxying State
~~~~~~~~~~~~~~

.. autofunction:: load_proxied_state_from_yaml

.. autoclass:: AirflowProxiedState

.. autoclass:: DagProxiedState

.. autoclass:: TaskProxiedState

Operators
~~~~~~~~~

.. autoclass:: BaseDagsterAssetsOperator

Task-level proxying
~~~~~~~~~~~~~~~~~~~

.. autoclass:: BaseProxyTaskToDagsterOperator

.. autoclass:: DefaultProxyTaskToDagsterOperator

Dag-level proxying
~~~~~~~~~~~~~~~~~~

.. autoclass:: BaseProxyDAGToDagsterOperator

.. autoclass:: DefaultProxyDAGToDagsterOperator


