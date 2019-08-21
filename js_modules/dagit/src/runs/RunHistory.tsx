import * as React from "react";
import gql from "graphql-tag";
import {
  NonIdealState,
  Menu,
  MenuItem,
  Colors,
  Icon,
  Popover,
  Button,
  Position,
  ButtonGroup,
  Spinner
} from "@blueprintjs/core";
import styled from "styled-components";
import { Link } from "react-router-dom";

import { RunHistoryRunFragment } from "./types/RunHistoryRunFragment";
import { titleForRun, RunStatus, IRunStatus } from "./RunUtils";
import { showCustomAlert } from "../CustomAlertProvider";

function dateString(timestamp: number) {
  if (timestamp === 0) {
    return null;
  }
  return new Date(timestamp).toLocaleString();
}

function elapsedTimeString(start: number, end?: number) {
  const s = ((end || Date.now()) - start) / 1000;
  return `${Math.ceil(s)} seconds`;
}

function getStartTime(run: RunHistoryRunFragment) {
  for (const log of run.logs.nodes) {
    if (log.__typename === "PipelineStartEvent") {
      return Number(log.timestamp);
    }
  }
  return 0;
}

function getEndTime(run: RunHistoryRunFragment) {
  for (const log of run.logs.nodes) {
    if (
      log.__typename === "PipelineSuccessEvent" ||
      log.__typename === "PipelineFailureEvent"
    ) {
      return Number(log.timestamp);
    }
  }
  return 0;
}

function getDetailedStats(run: RunHistoryRunFragment) {
  // 18 steps succeeded, 3 steps failed, 4 materializations 10 expectations, etc.
  const stats = {
    stepsSucceeded: 0,
    stepsFailed: 0,
    materializations: 0,
    expectationsSucceeded: 0,
    expectationsFailed: 0
  };
  for (const log of run.logs.nodes) {
    if (log.__typename === "ExecutionStepFailureEvent") stats.stepsFailed += 1;
    if (log.__typename === "ExecutionStepSuccessEvent")
      stats.stepsSucceeded += 1;
    if (log.__typename === "StepMaterializationEvent")
      stats.materializations += 1;
    if (log.__typename === "StepExpectationResultEvent") {
      if (log.expectationResult.success) {
        stats.expectationsSucceeded += 1;
      } else {
        stats.expectationsFailed += 1;
      }
    }
  }
  return stats;
}

enum RunSort {
  START_TIME_ASC,
  START_TIME_DSC,
  END_TIME_ASC,
  END_TIME_DSC
}

const AllRunStatuses: IRunStatus[] = [
  "NOT_STARTED",
  "STARTED",
  "SUCCESS",
  "FAILURE"
];

function sortLabel(sort: RunSort) {
  switch (sort) {
    case RunSort.START_TIME_ASC:
      return "Start Time (Asc)";
    case RunSort.START_TIME_DSC:
      return "Start Time (Desc)";
    case RunSort.END_TIME_ASC:
      return "End Time (Asc)";
    case RunSort.END_TIME_DSC:
      return "End Time (Desc)";
  }
}

interface IRunHistoryProps {
  runs: RunHistoryRunFragment[];
}

export default class RunHistory extends React.Component<
  IRunHistoryProps,
  { sort: RunSort; statuses: IRunStatus[] }
> {
  static fragments = {
    RunHistoryRunFragment: gql`
      fragment RunHistoryRunFragment on PipelineRun {
        runId
        status
        stepKeysToExecute
        mode
        environmentConfigYaml
        pipeline {
          name
          presets {
            name
            mode
            solidSubset
            environmentConfigYaml
          }
        }
        logs {
          nodes {
            __typename
            ... on MessageEvent {
              timestamp
            }
            ... on StepExpectationResultEvent {
              expectationResult {
                success
              }
            }
          }
        }
        executionPlan {
          steps {
            key
          }
        }
      }
    `
  };

  state = { sort: RunSort.START_TIME_DSC, statuses: AllRunStatuses };

  sortRuns = (runs: RunHistoryRunFragment[]) => {
    const sortType = this.state.sort;
    if (sortType === null) {
      return runs;
    }

    return runs.sort((a, b) => {
      switch (sortType) {
        case RunSort.START_TIME_ASC:
          return getStartTime(a) - getStartTime(b);
        case RunSort.START_TIME_DSC:
          return getStartTime(b) - getStartTime(a);
        case RunSort.END_TIME_ASC:
          return getEndTime(a) - getEndTime(b);
        case RunSort.END_TIME_DSC:
        default:
          return getEndTime(b) - getEndTime(a);
      }
    });
  };

  render() {
    const { runs } = this.props;

    const mostRecentRun = runs[runs.length - 1];
    const sortedRuns = this.sortRuns(runs.slice(0, runs.length - 1)).filter(r =>
      this.state.statuses.includes(r.status)
    );

    return (
      <RunsScrollContainer>
        {runs.length === 0 ? (
          <div style={{ marginTop: 100 }}>
            <NonIdealState
              icon="history"
              title="Pipeline Runs"
              description="No runs to display. Use the Execute tab to start a pipeline."
            />
          </div>
        ) : (
          <>
            <MostRecentRun run={mostRecentRun} />
            <RunTable
              runs={sortedRuns}
              sort={this.state.sort}
              statuses={this.state.statuses}
              onSetSort={sort => this.setState({ sort })}
              onSetStatuses={statuses => this.setState({ statuses })}
            />
          </>
        )}
      </RunsScrollContainer>
    );
  }
}

const MostRecentRun: React.FunctionComponent<{
  run: RunHistoryRunFragment;
}> = props => (
  <div>
    <Header style={{ marginTop: 0 }}>Most Recent Run</Header>
    <RunRow run={Object.assign({}, props.run, { status: "FAILURE" })} />
  </div>
);

interface RunTableProps {
  runs: RunHistoryRunFragment[];
  sort: RunSort;
  statuses: IRunStatus[];
  onSetStatuses: (statuses: IRunStatus[]) => void;
  onSetSort: (sort: RunSort) => void;
}

const RunTable: React.FunctionComponent<RunTableProps> = props => (
  <div>
    <Header>
      <div style={{ float: "right" }}>
        <ButtonGroup style={{ marginRight: 15 }}>
          {AllRunStatuses.map(s => (
            <Button
              key={s}
              active={props.statuses.includes(s)}
              onClick={() =>
                props.onSetStatuses(
                  props.statuses.includes(s)
                    ? props.statuses.filter(a => a !== s)
                    : props.statuses.concat([s])
                )
              }
            >
              {s === "STARTED" ? (
                <Spinner size={11} value={0.4} />
              ) : (
                <RunStatus status={s} />
              )}
            </Button>
          ))}
        </ButtonGroup>
        <Popover
          position={Position.BOTTOM_RIGHT}
          content={
            <Menu>
              {[
                RunSort.START_TIME_ASC,
                RunSort.START_TIME_DSC,
                RunSort.END_TIME_ASC,
                RunSort.END_TIME_DSC
              ].map((v, idx) => (
                <MenuItem
                  key={idx}
                  text={sortLabel(v)}
                  onClick={() => props.onSetSort(v)}
                />
              ))}
            </Menu>
          }
        >
          <Button
            icon={"sort"}
            rightIcon={"caret-down"}
            text={sortLabel(props.sort)}
          />
        </Popover>
      </div>
      {`Previous Runs (${props.runs.length})`}
    </Header>
    <Legend>
      <LegendColumn style={{ maxWidth: 40 }}></LegendColumn>
      <LegendColumn style={{ flex: 2.3 }}>Run ID</LegendColumn>
      <LegendColumn>Pipeline</LegendColumn>
      <LegendColumn>Config</LegendColumn>
      <LegendColumn style={{ flex: 1.6 }}>Timing</LegendColumn>
    </Legend>
    {props.runs.map(run => (
      <RunRow run={run} key={run.runId} />
    ))}
  </div>
);

const RunRow: React.FunctionComponent<{ run: RunHistoryRunFragment }> = ({
  run
}) => {
  const start = getStartTime(run);
  const end = getEndTime(run);
  const stats = getDetailedStats(run);
  const preset = run.pipeline.presets.find(
    preset =>
      preset.mode === run.mode &&
      preset.environmentConfigYaml === run.environmentConfigYaml
  );

  return (
    <RunRowContainer key={run.runId}>
      <RunRowColumn
        style={{ maxWidth: 30, paddingLeft: 0, textAlign: "center" }}
      >
        <RunStatus status={start && end ? run.status : "STARTED"} />
      </RunRowColumn>
      <RunRowColumn style={{ flex: 2.4 }}>
        <Link style={{ display: "block" }} to={`/runs/${run.runId}`}>
          {titleForRun(run)}
        </Link>
        <RunDetails>
          {`${stats.stepsSucceeded}/${stats.stepsSucceeded +
            stats.stepsFailed} steps succeeded, `}
          <Link
            to={`/runs/${run.runId}?q=type:materialization`}
          >{`${stats.materializations} materializations`}</Link>
          ,{" "}
          <Link to={`/runs/${run.runId}?q=type:expectation`}>{`${
            stats.expectationsSucceeded
          }/${stats.expectationsSucceeded +
            stats.expectationsFailed} expectations passed`}</Link>
        </RunDetails>
      </RunRowColumn>
      <RunRowColumn>
        <Link style={{ display: "block" }} to={`/${run.pipeline.name}/explore`}>
          <Icon icon="diagram-tree" /> {run.pipeline.name}
        </Link>
      </RunRowColumn>
      <RunRowColumn>
        <a
          title="View configuration..."
          style={{
            display: "flex",
            alignItems: "baseline"
          }}
          onClick={() =>
            showCustomAlert({
              title: "Config",
              message: run.environmentConfigYaml,
              messageLang: ["yaml"],
              pre: true
            })
          }
        >
          <span style={{ paddingRight: 7 }}>
            {preset ? `Preset: ${preset.name}` : "Custom"}
          </span>
          <Icon iconSize={12} icon="share" />
        </a>
      </RunRowColumn>
      <RunRowColumn style={{ flex: 1.6 }}>
        {start ? (
          <div style={{ marginBottom: 4 }}>
            <Icon icon="calendar" /> {dateString(start)}
            <Icon
              icon="arrow-right"
              style={{ marginLeft: 10, marginRight: 10 }}
            />
            {dateString(end)}
          </div>
        ) : (
          <div style={{ marginBottom: 4 }}>
            <Icon icon="calendar" /> Starting...
          </div>
        )}
        <div>
          <Icon icon="time" /> {elapsedTimeString(start, end)}
        </div>
      </RunRowColumn>
    </RunRowContainer>
  );
};

const Header = styled.div`
  color: ${Colors.BLACK};
  font-size: 1.1rem;
  line-height: 3rem;
  margin-top: 40px;
`;
const Legend = styled.div`
  display: flex;
  margin-bottom: 9px;
`;
const LegendColumn = styled.div`
  flex: 1;
  padding-left: 10px;
  color: #8a9ba8;
  text-transform: uppercase;
  font-size: 11px;
`;
const RunRowContainer = styled.div`
  display: flex;
  background: ${Colors.WHITE};
  color: ${Colors.DARK_GRAY5};
  border-bottom: 1px solid ${Colors.LIGHT_GRAY1};
  margin-bottom: 9px;
  box-shadow: 0 1px 1px rgba(0, 0, 0, 0.2);
  padding: 2px 10px;
  text-decoration: none;
`;
const RunRowColumn = styled.div`
  flex: 1;
  padding: 7px 10px;
  border-right: 1px solid ${Colors.LIGHT_GRAY3};
  &:last-child {
    border-right: none;
  }
`;
const RunsScrollContainer = styled.div`
  background-color: rgb(245, 248, 250);
  padding: 20px;
  overflow: scroll;
  min-height: calc(100vh - 50px);
`;
const RunDetails = styled.div`
  font-size: 0.8rem;
  margin-top: 4px;
`;
