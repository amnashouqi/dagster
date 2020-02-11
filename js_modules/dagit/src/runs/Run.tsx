import * as React from "react";
import * as yaml from "yaml";
import gql from "graphql-tag";
import styled from "styled-components/macro";
import { Colors } from "@blueprintjs/core";
import { MutationFunction, Mutation } from "react-apollo";
import ApolloClient from "apollo-client";

import {
  ILogFilter,
  LogsProvider,
  GetDefaultLogFilter,
  structuredFieldsFromLogFilter
} from "./LogsProvider";
import LogsScrollingTable from "./LogsScrollingTable";
import { RunFragment, RunFragment_executionPlan } from "./types/RunFragment";
import { SplitPanelContainer, SplitPanelToggles } from "../SplitPanelContainer";
import { RunMetadataProvider, IStepState } from "../RunMetadataProvider";
import LogsToolbar from "./LogsToolbar";
import {
  handleExecutionResult,
  START_PIPELINE_EXECUTION_MUTATION
} from "./RunUtils";
import {
  StartPipelineExecution,
  StartPipelineExecutionVariables
} from "./types/StartPipelineExecution";
import { RunStatusToPageAttributes } from "./RunStatusToPageAttributes";
import InfoModal from "../InfoModal";
import PythonErrorInfo from "../PythonErrorInfo";
import { RunContext } from "./RunContext";

import {
  RunPipelineRunEventFragment_ExecutionStepFailureEvent,
  RunPipelineRunEventFragment
} from "./types/RunPipelineRunEventFragment";
import { GaantChart, GaantChartMode } from "../gaant/GaantChart";
import { RunActionButtons } from "./RunActionButtons";

interface IRunProps {
  client: ApolloClient<any>;
  run?: RunFragment;
}

interface IRunState {
  logsFilter: ILogFilter;
  highlightedError?: { message: string; stack: string[] };
}

export class Run extends React.Component<IRunProps, IRunState> {
  static fragments = {
    RunFragment: gql`
      fragment RunFragment on PipelineRun {
        ...RunStatusPipelineRunFragment

        environmentConfigYaml
        runId
        canCancel
        status
        mode
        pipeline {
          __typename
          ... on PipelineReference {
            name
          }
          ... on Pipeline {
            solids {
              name
            }
          }
        }
        executionPlan {
          steps {
            key
            inputs {
              dependsOn {
                key
                outputs {
                  name
                  type {
                    name
                  }
                }
              }
            }
          }
          artifactsPersisted
          ...GaantChartExecutionPlanFragment
        }
        stepKeysToExecute
      }

      ${RunStatusToPageAttributes.fragments.RunStatusPipelineRunFragment}
      ${GaantChart.fragments.GaantChartExecutionPlanFragment}
    `,
    RunPipelineRunEventFragment: gql`
      fragment RunPipelineRunEventFragment on PipelineRunEvent {
        ... on MessageEvent {
          message
          timestamp
          level
          step {
            key
          }
        }
        ... on ExecutionStepFailureEvent {
          error {
            ...PythonErrorFragment
          }
        }
        ...LogsScrollingTableMessageFragment
        ...RunMetadataProviderMessageFragment
      }

      ${RunMetadataProvider.fragments.RunMetadataProviderMessageFragment}
      ${LogsScrollingTable.fragments.LogsScrollingTableMessageFragment}
      ${PythonErrorInfo.fragments.PythonErrorFragment}
    `
  };

  state: IRunState = {
    logsFilter: GetDefaultLogFilter(),
    highlightedError: undefined
  };

  onShowStateDetails = (
    stepKey: string,
    logs: RunPipelineRunEventFragment[]
  ) => {
    const errorNode = logs.find(
      node =>
        node.__typename === "ExecutionStepFailureEvent" &&
        node.step != null &&
        node.step.key === stepKey
    ) as RunPipelineRunEventFragment_ExecutionStepFailureEvent;

    if (errorNode) {
      this.setState({ highlightedError: errorNode.error });
    }
  };

  onReexecute = async (
    mutation: MutationFunction<
      StartPipelineExecution,
      StartPipelineExecutionVariables
    >,
    stepKey?: string,
    resumeRetry?: boolean
  ) => {
    const { run } = this.props;

    if (!run || run.pipeline.__typename === "UnknownPipeline") return;

    const variables: StartPipelineExecutionVariables = {
      executionParams: {
        mode: run.mode,
        environmentConfigData: yaml.parse(run.environmentConfigYaml),
        selector: {
          name: run.pipeline.name,
          solidSubset: run.pipeline.solids.map(s => s.name)
        }
      }
    };

    if (stepKey && run.executionPlan) {
      const step = run.executionPlan.steps.find(s => s.key === stepKey);
      if (!step) return;
      variables.executionParams.stepKeys = [stepKey];
      variables.executionParams.retryRunId = run.runId;
    } else if (resumeRetry) {
      variables.executionParams.retryRunId = run.runId;
    }

    const result = await mutation({ variables });

    handleExecutionResult(run.pipeline.name, result, {
      openInNewWindow: false
    });
  };

  render() {
    const { client, run } = this.props;
    const { logsFilter, highlightedError } = this.state;

    return (
      <RunContext.Provider value={run}>
        {run && <RunStatusToPageAttributes run={run} />}
        {highlightedError && (
          <InfoModal
            onRequestClose={() =>
              this.setState({ highlightedError: undefined })
            }
          >
            <PythonErrorInfo error={highlightedError} />
          </InfoModal>
        )}

        <Mutation<StartPipelineExecution, StartPipelineExecutionVariables>
          mutation={START_PIPELINE_EXECUTION_MUTATION}
        >
          {reexecuteMutation => (
            <LogsProvider
              client={client}
              runId={run ? run.runId : ""}
              filter={logsFilter}
            >
              {({ filteredNodes, allNodes, loaded }) => (
                <RunWithData
                  run={run}
                  filteredNodes={filteredNodes}
                  allNodes={allNodes}
                  logsLoading={!loaded}
                  logsFilter={logsFilter}
                  onSetLogsFilter={logsFilter => this.setState({ logsFilter })}
                  onShowStateDetails={this.onShowStateDetails}
                  onReexecute={(...args) =>
                    this.onReexecute(reexecuteMutation, ...args)
                  }
                />
              )}
            </LogsProvider>
          )}
        </Mutation>
      </RunContext.Provider>
    );
  }
}

interface RunWithDataProps {
  run?: RunFragment;
  allNodes: (RunPipelineRunEventFragment & { clientsideKey: string })[];
  filteredNodes: (RunPipelineRunEventFragment & { clientsideKey: string })[];
  logsFilter: ILogFilter;
  logsLoading: boolean;
  onSetLogsFilter: (v: ILogFilter) => void;
  onShowStateDetails: (
    stepKey: string,
    logs: RunPipelineRunEventFragment[]
  ) => void;
  onReexecute: (stepKey?: string, resumeRetry?: boolean) => Promise<void>;
}

const RunWithData = ({
  run,
  allNodes,
  filteredNodes,
  logsFilter,
  logsLoading,
  onReexecute,
  onSetLogsFilter
}: RunWithDataProps) => {
  const splitPanelContainer = React.createRef<SplitPanelContainer>();

  const selectedStep = structuredFieldsFromLogFilter(logsFilter).step;
  const executionPlan: RunFragment_executionPlan = run?.executionPlan || {
    __typename: "ExecutionPlan",
    steps: [],
    artifactsPersisted: false
  };

  return (
    <RunMetadataProvider logs={allNodes}>
      {metadata => (
        <SplitPanelContainer
          ref={splitPanelContainer}
          axis={"vertical"}
          identifier="run-gaant"
          firstInitialPercent={35}
          firstMinSize={40}
          first={
            <GaantChart
              options={{
                mode: GaantChartMode.WATERFALL_TIMED
              }}
              toolbarLeftActions={
                <SplitPanelToggles
                  axis={"vertical"}
                  container={splitPanelContainer}
                />
              }
              toolbarActions={
                <RunActionButtons
                  run={run}
                  artifactsPersisted={executionPlan.artifactsPersisted}
                  onReexecute={onReexecute}
                  selectedStep={selectedStep}
                  selectedStepState={
                    (selectedStep && metadata.steps[selectedStep]?.state) ||
                    IStepState.WAITING
                  }
                />
              }
              plan={executionPlan}
              metadata={metadata}
              selectedStep={selectedStep}
              onApplyStepFilter={stepKey =>
                onSetLogsFilter({ ...logsFilter, text: `step:${stepKey}` })
              }
            />
          }
          second={
            <LogsContainer>
              <LogsToolbar
                showSpinner={false}
                onSetFilter={onSetLogsFilter}
                filter={logsFilter}
                filterStep={selectedStep}
                filterStepState={
                  (selectedStep && metadata.steps[selectedStep]?.state) ||
                  IStepState.WAITING
                }
              />
              <LogsScrollingTable
                nodes={filteredNodes}
                loading={logsLoading}
                filterKey={JSON.stringify(logsFilter)}
              />
            </LogsContainer>
          }
        />
      )}
    </RunMetadataProvider>
  );
};

const LogsContainer = styled.div`
  display: flex;
  flex-direction: column;
  height: 100%;
  background: ${Colors.LIGHT_GRAY5};
`;
