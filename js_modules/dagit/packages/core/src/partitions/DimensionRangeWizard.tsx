import {
  Box,
  Button,
  Checkbox,
  Colors,
  Icon,
  Menu,
  MenuDivider,
  MenuItem,
  TagSelectorWithSearch,
} from '@dagster-io/ui';
import * as React from 'react';
import styled from 'styled-components/macro';

import {StateDot} from '../assets/AssetPartitionList';
import {partitionStateAtIndex, Range} from '../assets/usePartitionHealthData';
import {PartitionDefinitionType} from '../graphql/types';
import {RepoAddress} from '../workspace/types';

import {CreatePartitionDialog} from './CreatePartitionDialog';
import {DimensionRangeInput} from './DimensionRangeInput';
import {PartitionStatusHealthSource, PartitionStatus} from './PartitionStatus';

export const DimensionRangeWizard: React.FC<{
  selected: string[];
  setSelected: (selected: string[]) => void;
  partitionKeys: string[];
  health: PartitionStatusHealthSource;
  dimensionType: PartitionDefinitionType;
  partitionDefinitionName?: string | null;
  repoAddress?: RepoAddress;
  refetch?: () => Promise<void>;
}> = ({
  selected,
  setSelected,
  partitionKeys,
  health,
  dimensionType,
  partitionDefinitionName,
  repoAddress,
  refetch,
}) => {
  const isTimeseries = dimensionType === PartitionDefinitionType.TIME_WINDOW;
  const isDynamic = dimensionType === PartitionDefinitionType.DYNAMIC;

  const [showCreatePartition, setShowCreatePartition] = React.useState(false);

  return (
    <>
      <Box flex={{direction: 'row', alignItems: 'center', gap: 8}} padding={{vertical: 4}}>
        <Box flex={{direction: 'column'}} style={{flex: 1}}>
          {isTimeseries ? (
            <DimensionRangeInput
              value={selected}
              partitionKeys={partitionKeys}
              onChange={setSelected}
              isTimeseries={isTimeseries}
            />
          ) : (
            <OrdinalPartitionSelector
              allPartitions={partitionKeys}
              selectedPartitions={selected}
              setSelectedPartitions={setSelected}
              health={health}
              setShowCreatePartition={setShowCreatePartition}
              isDynamic={isDynamic}
            />
          )}
        </Box>
        {isTimeseries && (
          <Button small={true} onClick={() => setSelected(partitionKeys.slice(-1))}>
            Latest
          </Button>
        )}
        <Button small={true} onClick={() => setSelected(partitionKeys)}>
          All
        </Button>
      </Box>
      <Box margin={{bottom: 8}}>
        {isDynamic && (
          <LinkText
            flex={{direction: 'row', alignItems: 'center', gap: 8}}
            onClick={() => {
              setShowCreatePartition(true);
            }}
          >
            <StyledIcon name="add" size={24} />
            <div>Add a partition</div>
          </LinkText>
        )}
        {isTimeseries && (
          <PartitionStatus
            partitionNames={partitionKeys}
            health={health}
            splitPartitions={!isTimeseries}
            selected={selected}
            onSelect={setSelected}
          />
        )}
      </Box>
      {repoAddress && (
        <CreatePartitionDialog
          key={showCreatePartition ? '1' : '0'}
          isOpen={showCreatePartition}
          partitionDefinitionName={partitionDefinitionName}
          repoAddress={repoAddress}
          close={() => {
            setShowCreatePartition(false);
          }}
          refetch={refetch}
          onCreated={(partitionName) => {
            setSelected([...selected, partitionName]);
          }}
        />
      )}
    </>
  );
};

const OrdinalPartitionSelector: React.FC<{
  allPartitions: string[];
  selectedPartitions: string[];
  setSelectedPartitions: (tags: string[]) => void;
  health: PartitionStatusHealthSource;
  setShowCreatePartition: (show: boolean) => void;
  isDynamic: boolean;
}> = ({
  allPartitions,
  selectedPartitions,
  setSelectedPartitions,
  setShowCreatePartition,
  isDynamic,
  health,
}) => {
  const statusForPartitionKey = (partitionKey: string) => {
    const index = allPartitions.indexOf(partitionKey);
    if ('ranges' in health) {
      return partitionStateAtIndex(health.ranges as Range[], index);
    } else {
      return health.partitionStateForKey(partitionKey, index);
    }
  };

  return (
    <>
      <TagSelectorWithSearch
        allTags={allPartitions}
        selectedTags={selectedPartitions}
        setSelectedTags={setSelectedPartitions}
        placeholder="Select a partition or create one"
        renderDropdownItem={(tag, dropdownItemProps) => {
          return (
            <label>
              <MenuItem
                tagName="div"
                text={
                  <Box flex={{alignItems: 'center', gap: 12}}>
                    <Checkbox
                      checked={dropdownItemProps.selected}
                      onChange={dropdownItemProps.toggle}
                    />
                    <StateDot state={statusForPartitionKey(tag)} />
                    <span>{tag}</span>
                  </Box>
                }
              />
            </label>
          );
        }}
        renderDropdown={(dropdown, {width, allTags}) => {
          const isAllSelected = allTags.every((t) => selectedPartitions.includes(t));
          return (
            <Menu style={{width}}>
              <Box padding={4}>
                {isDynamic && (
                  <>
                    <Box flex={{direction: 'column'}}>
                      <MenuItem
                        tagName="div"
                        text={
                          <Box flex={{direction: 'row', alignItems: 'center', gap: 12}}>
                            <StyledIcon name="add" size={24} />
                            <span>Add partition</span>
                          </Box>
                        }
                        onClick={() => {
                          setShowCreatePartition(true);
                        }}
                      />
                    </Box>
                    <MenuDivider />
                  </>
                )}
                {allTags.length ? (
                  <>
                    <label>
                      <MenuItem
                        tagName="div"
                        text={
                          <Box flex={{alignItems: 'center', gap: 12}}>
                            <Checkbox
                              checked={isAllSelected}
                              onChange={() => {
                                if (isAllSelected) {
                                  setSelectedPartitions([]);
                                } else {
                                  setSelectedPartitions(allTags);
                                }
                              }}
                            />
                            <span>Select all ({allTags.length})</span>
                          </Box>
                        }
                      />
                    </label>
                    {dropdown}
                  </>
                ) : (
                  <div style={{padding: '6px 6px 0px 6px', color: Colors.Gray700}}>
                    No matching partitions found
                  </div>
                )}
              </Box>
            </Menu>
          );
        }}
        renderTagList={(tags) => {
          if (tags.length > 4) {
            return <span>{tags.length} partitions selected</span>;
          }
          return tags;
        }}
        searchPlaceholder="Filter partitions"
      />
    </>
  );
};

const StyledIcon = styled(Icon)`
  font-weight: 500;
`;

const LinkText = styled(Box)`
  color: ${Colors.Link};
  cursor: pointer;
  &:hover {
    text-decoration: underline;
  }
  > * {
    height: 24px;
    align-content: center;
    line-height: 24px;
  }
`;
