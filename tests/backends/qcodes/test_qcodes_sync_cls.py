# test QCoDeSSync

# create a mock qcodes database -- in session fixture
# --> create QCoDeSConfigData object, with the path, dummy set_up, dummy static_attributes

# create a 2nd mock of a second database.

# getNewDatasets
#  --> test --> initial test, last_sync_item = None
#  --> test --> test with last_sync_item = SyncItems(dataIdentifier=str(row[0]), syncPriority=row[0])

# --> add a single measurement to the database --> check if 1 result is returned and run_id = 1
# --> add a second measurement to the database --> check if 2 results are returned and run_id = 1,2
# --> add a partial measurement that is not yet complete --> check if 3 results are returned and run_id = 1,2,3
# --> this should be done by just creating a manual measurement loop and then running the getNewDatasets function in the middle.
# let the measurement finish.

# syncDatasetNormal
# provide the syncDatasetNormal
# --> check --> if expected attributes and tags are present in the dataset. the run timestamp should match with the collected datetime
