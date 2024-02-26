
from dbs.apis.dbsClient import DbsApi
#migUrl='https://cmsweb-testbed.cern.ch/dbs/int/phys03/DBSMigrate'
#phy3Url = 'https://cmsweb-testbed.cern.ch/dbs/int/phys03/DBSReader'
migUrl='https://cmsweb-production.cern.ch/dbs/prod/phys03/DBSMigrate'
phy3Url = 'https://cmsweb-production.cern.ch/dbs/prod/phys03/DBSReader'
globUrl='https://cmsweb-productio.cern.ch/dbs/prod/global/DBSReader'
apiG   = DbsApi(url=globUrl)
apiP3  = DbsApi(url=phy3Url)
apiMig = DbsApi(url=migUrl)

# pick  a dataset
dataset='/NonPrD0_pT-1p2_y-2p4_pp_13TeV_pythia8/RunIILowPUAutumn18DR-102X_upgrade2018_realistic_v15-v1/AODSIM'

#find blocks
bList=[]
for dic in apiG.listBlocks(dataset=dataset):
    bl1.append(dic['block_name'])

#submit migrations and collect their Id's
migrIds=[]
for b in bList:
    data= {'migration_url': globUrl, 'migration_input': b}
    result = apiMig.submitMigration(data)
    id = result.get("migration_details", {}).get("migration_request_id")
    migrIds.append(id)

#check status, to be repeated until OK (state=2) or terminally failed (state=9)
# https://github.com/dmwm/DBS/blob/625525721c3df9a9b842c84ad0ac29f55180524b/Client/src/python/dbs/apis/dbsClient.py#L1489-L1492
for id in migrIds:
        status = apiMig.statusMigration(migration_rqst_id=id)
        state = status[0].get("migration_status")
        retry_count = status[0].get("retry_count")
        print "migration id, state, retries : %d %d %s" % (id, state, retry_count)


# if there are old, failed migrations, need remove them in order to
# submit them again, the first loop will simly collect their ID's
for mig in migrIds:
    mdic={'migration_rqst_id':mid}
    apiMig.removeMigration({'migration_rqst_id':mid})

