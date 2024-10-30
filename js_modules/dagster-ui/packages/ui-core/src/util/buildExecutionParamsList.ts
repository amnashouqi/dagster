import * as yaml from 'yaml';

import {showCustomAlert} from '../app/CustomAlertProvider';
import {ExecutionParams, SensorSelector} from '../graphql/types';
import {sanitizeConfigYamlString} from '../launchpad/yamlUtils';
import {SensorDryRunInstigationTick} from '../ticks/types/SensorDryRunDialog.types';

const YAML_SYNTAX_INVALID = `The YAML you provided couldn't be parsed. Please fix the syntax errors and try again.`;

// This helper removes __typename, which prevents tags from being passed back to GraphQL
const onlyKeyAndValue = ({key, value}: {key: string; value: string}) => ({key, value});

// adapted from buildExecutionVariables() in LaunchpadSession.tsx
export const buildExecutionParamsListSensor = (
  sensorExecutionData: SensorDryRunInstigationTick,
  sensorSelector: SensorSelector,
) => {
  if (!sensorExecutionData) {
    return [];
  }

  const executionParamsList: ExecutionParams[] = [];

  sensorExecutionData?.evaluationResult?.runRequests?.forEach((request) => {
    const configYamlOrEmpty = sanitizeConfigYamlString(request.runConfigYaml);

    try {
      yaml.parse(configYamlOrEmpty);
    } catch {
      showCustomAlert({title: 'Invalid YAML', body: YAML_SYNTAX_INVALID});
      return;
    }
    const {repositoryLocationName, repositoryName} = sensorSelector;

    const executionParams: ExecutionParams = {
      runConfigData: configYamlOrEmpty,
      selector: {
        jobName: request.jobName, // get jobName from runRequest
        repositoryLocationName,
        repositoryName,
        assetSelection: [],
        assetCheckSelection: [],
        solidSelection: undefined,
      },
      mode: 'default',
      executionMetadata: {
        tags: [...request.tags.map(onlyKeyAndValue)],
      },
    };
    executionParamsList.push(executionParams);
  });
  return executionParamsList;
};
