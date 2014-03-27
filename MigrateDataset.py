#!/usr/bin/env python

import time
from dbs.apis.dbsClient import DbsApi

instance = 'dev'
instance = 'int'
instance = 'prod'


dataset = '/MinBias_TuneA2MB_13TeV-pythia8/Fall13-POSTLS162_V1-v1/GEN-SIM'

if instance=='dev':
    host = 'dbs3-dev01.cern.ch'
if instance=='int':
    host = 'cmsweb-testbed.cern.ch'
if instance=='prod':
    host = 'cmsweb.cern.ch'

globUrl = 'https://%s/dbs/%s/global/DBSReader' % (host, instance)

phy3Url = 'https://%s/dbs/%s/phys03/DBSReader' % (host, instance)
migUrl  = 'https://%s/dbs/%s/phys03/DBSMigrate' % (host, instance)

apiG   = DbsApi(url=globUrl)
apiP3  = DbsApi(url=phy3Url)
apiMig = DbsApi(url=migUrl)

dsg = apiG.listDatasets(dataset=dataset)
print "in Global: ", dsg

ds3 = apiP3.listDatasets(dataset=dataset)
print "in Phys03 : ", ds3

data = {'migration_url': globUrl, 'migration_input': dataset}
result = apiMig.submitMigration(data)

print result

id = result.get("migration_details", {}).get("migration_request_id")
report = result.get("migration_report")

print "migration id, report: ", id, report

migrationIds = [id]

wait=2
while len(migrationIds) > 0:
    if wait > 1:
        msg=" %d Block migrations in progress. Next check in %d sec. " % (len(migrationIds), wait)
        if wait > 10:
            msg+="\n     You can Ctl-C out at any time and re-issue crab -publish later,"
            msg+="\n     migrations will continue in background"
        print msg
    time.sleep(wait)
    idToCheck=migrationIds[:]  # copy, not reference
    for id in idToCheck:
        status = apiMig.statusMigration(migration_rqst_id=id)
        try:
            state = status[0].get("migration_status")
            retry_count = status[0].get("retry_count")
        except:
            msg="ERROR: Can't get status for migration_id %d" % id
            print msg
            raise exception

        if state == 2:
            print "Migration id %d has succeeded" % id
            migrationIds.remove(id)
        if state == 9:
            print "Migration id %d terminally FAILED. State = 9." % id
            print "Full status for migration id %d:\n%s" % (id, str(status))
            migrationIds.remove(id)
        if state == 3:
            if retry_count == 3:
                print "Migration id %d has failed" % id
                print "Full status for migration id %d:\n%s" % (id, str(status))
                migrationIds.remove(id)
            else:
                common.logger.info("Migration id %d will be retried" % id)
                pass
        if state == 0 or state == 1:
            pass
    wait=min(wait+20,120)  # give it more time, but check every 2 minutes at least

print "Migration of %s is complete." % dataset


ds3 = apiP3.listDatasets(dataset=dataset)
print "in Phys03 : ", ds3
