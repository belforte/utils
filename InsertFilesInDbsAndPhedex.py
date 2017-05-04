#!/usr/bin/env python
"""
Example script to inject user files in DBS3.

Intendend for LHE files and for sub-detector data.

For authentication you need a valid proxy

voms-proxy-init -voms cms

You also will need the WMCore python libs, the DBS3 API
and some other WMAgent externals in the PYTHONPATH.

Useful information (feedback from Stefano and Yuyi):
 - acquisition_era: it must be LHE (or CRAB) to bypass the name validation.
 - create_by: DBS3 will automatically fill this field
 - creation_date: DBS3 will automatically fill this field

Injection into PhEDEx added by Dirk H.

"""

import sys
import json
import uuid
import time
import pprint
import logging

from optparse import OptionParser

from dbs.apis.dbsClient import DbsApi
from WMCore.Services.PhEDEx import XMLDrop
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx


def insertFilesToBlock(files, injectNode, injectSE, mode, commit):

    # pick a DBS3 instance
    # instance = 'dev'
    instance = 'int'
    # instance = 'prod'

    if instance=='dev':
        # host = 'dbs3-dev01.cern.ch'
        host = 'cmsweb-dev.cern.ch'

    if instance=='int':
        host = 'cmsweb-testbed.cern.ch'

    if instance=='prod':
        host = 'cmsweb.cern.ch'

    globReadUrl = 'https://%s/dbs/%s/global/DBSReader' % (host, instance)
    globWriteUrl = 'https://%s/dbs/%s/global/DBSWriter' % (host, instance)
    phy3ReadUrl = 'https://%s/dbs/%s/phys03/DBSReader' % (host, instance)
    phy3WriteUrl = 'https://%s/dbs/%s/phys03/DBSWriter' % (host, instance)

    readApi   = DbsApi(url=globReadUrl)
    writeApi  = DbsApi(url=globWriteUrl)
    # readApi   = DbsApi(url=phy3ReadUrl)
    # writeApi  = DbsApi(url=phy3WriteUrl)

    if mode == "lhe":
        ds_info = {
            'data_type'       : 'mc',
            'acquisition_era' : 'LHE',
            'primary_ds'      : 'QCD_HT-100To250_8TeV-madgraph',
            'processed_ds'    : 'LHE-testAlan_Attempt3-v2',
            'data_tier'       : 'LHE',
            'physics_group'   : 'GEN',
            'application'     : 'Madgraph',
            'app_version'     : 'Mad_5_1_3_30',
            'proc_version'    : 1,
            'proc_descript'   : 'test_LHE_injection'
            }
    elif mode == "pixel":
        ds_info = {
            'data_type'       : 'data',
            'acquisition_era' : 'Run2012',
            'primary_ds'      : 'QCD_HT-100To250_8TeV-madgraph',
            'processed_ds'    : 'LHE-testAlan_Attempt3-v2',
            'data_tier'       : 'LHE',
            'physics_group'   : None,
            'application'     : 'Madgraph',
            'app_version'     : 'Mad_5_1_3_30',
            'proc_version'    : 1,
            'proc_descript'   : 'test_LHE_injection'
            }

    acquisition_era_config = { 'acquisition_era_name' : ds_info['acquisition_era'],
                               'start_date' : int(time.time())
                               }

    processing_era_config = { 'processing_version': ds_info['proc_version'],
                              'description': ds_info['proc_descript']
                              }

    primds_config = { 'primary_ds_type': ds_info['data_type'],
                      'primary_ds_name': ds_info['primary_ds']
                      }

    dataset_name = "/%s/%s/%s" % (ds_info['primary_ds'], ds_info['processed_ds'], ds_info['data_tier'])
    dataset_config = { 'physics_group_name' : ds_info['physics_group'],
                       'dataset_access_type' : 'VALID',
                       'data_tier_name' : ds_info['data_tier'],
                       'processed_ds_name' : ds_info['processed_ds'],
                       'dataset' : dataset_name
                       }

    block_name = "%s#%s" % (dataset_name, str(uuid.uuid4()))
    block_config = { 'block_name' : block_name,
                     'origin_site_name' : injectSE,
                     'open_for_writing' : 0
                     }

    dataset_conf_list = [ { 'app_name' : ds_info['application'],
                            'global_tag' : 'dummytag',
                            'output_module_label' : 'out',
                            'pset_hash' : 'dummyhash',
                            'release_version' : ds_info['app_version']
                            } ]

    blockDict = { 'files': files,
                  'processing_era': processing_era_config,
                  'primds': primds_config,
                  'dataset': dataset_config,
                  'dataset_conf_list' : dataset_conf_list,
                  'acquisition_era': acquisition_era_config,
                  'block': block_config,
                  'file_parent_list':[],
                  'file_conf_list':[]
                  }

    blockDict['files'] = files
    blockDict['block']['file_count'] = len(files)
    blockDict['block']['block_size'] = sum([int(file['file_size']) for file in files])

    if commit:
        logging.info("inserted block into DBS : %s" % writeApi.url)
        logging.debug(pprint.pformat(blockDict))
        writeApi.insertBulkBlock(blockDict)
    else:
        logging.info("dry run, this block would have been inserted into DBS : %s" % writeApi.url)
        logging.info(pprint.pformat(blockDict))

    injectionSpec = XMLDrop.XMLInjectionSpec(writeApi.url)
    datasetSpec = injectionSpec.getDataset(dataset_name)
    blockSpec = datasetSpec.getFileblock(block_name, "n")
    for f in files:
        blockSpec.addFile(f['logical_file_name'],
                          { 'adler32' : f['adler32'] },
                          f['file_size'])

    xmlData = injectionSpec.save()

    # SELECT PHEDEX URL
    phedexURL = "https://cmsweb.cern.ch/phedex/datasvc/json/dev/"
    #phedexURL = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/"

    if commit:
        logging.info("inserting block into PhEDEx : %s" % phedexURL)
        logging.debug(pprint.pformat(xmlData))
        phedex = PhEDEx({"endpoint": config.PhEDExInjector.phedexurl}, "json")
        injectRes = phedex.injectBlocks(injectNode, xmlData)
    else:
        logging.info("dry run, this block would have been inserted into PhEDEx : %s" % phedexURL)
        logging.info(pprint.pformat(xmlData))

    return


def insertFiles(fileDictList, injectNode, injectSE, mode, commit):

    # PREPARE COMMON ADDITIONAL INFORMATION FOR FILES
    common_file_type  = 'LHE'
    common_dummy_lumi = [{'lumi_section_num': 1, 'run_num': 1}]

    # LOOP ON ALL FILES, CREATE BLOCKS AND INSERT THEM
    files = []
    max_files_in_block=500
    for f in fileDictList:

        fileDict = { 'file_type' : common_file_type,
                     'logical_file_name' : f['lfn'],
                     'check_sum' : f['check_sum'],
                     'adler32' : f['adler32'],
                     'file_size' : f['file_size'],
                     'event_count' : f['event_count'],
                     'file_lumi_list' : common_dummy_lumi
                     }

        files.append(fileDict)

        if len(files) == max_files_in_block:
            insertFilesToBlock(files, injectNode, injectSE, mode, commit)
            files = []

        # end loop on input Files

    # any leftovers ?
    if len(files) > 0:
        insertFilesToBlock(files, injectNode, injectSE, mode, commit)

    return


def main(argv=None):
    """
    _main_

    Parse the options and register files in DBS/PhEDEx

    """
    allowedModes = [ "lhe" ]

    usage = "Usage: %prog [options]"
    parser = OptionParser(usage = usage)
    parser.add_option("--jsonfile", dest="jsonfiles", action = "append",
                      help="contains file names plus metadata (multiple possible)", metavar="FILE")
    parser.add_option("--injectnode", dest="injectnode",
                      help="PhEDEx node the files should be injected at", metavar="NODE")
    parser.add_option("--injectse", dest="injectse",
                      help="SE the files should be injected at", metavar="SE")
    parser.add_option("--eos", dest="eos", action = "store_true", default = False,
                      help="Shortcut to set InjectNode and InjectSE to CERN EOS")
    parser.add_option("--castor", dest="castor", action = "store_true", default = False,
                      help="Shortcut to set InjectNode and InjectSE to CERN Castor")
    parser.add_option("--mode", dest="mode",
                      help="Use mode, allowed are %s" % allowedModes, metavar="MODE")
    parser.add_option("--commit", dest="commit", action = "store_true", default = False,
                      help="Really write to DBS and PhEDEx ?")
    parser.add_option("-v", "--verbose", action = "store_true", default = False,
                      dest = "verbose", help = "Prints DEBUG logging statements")
    parser.add_option("-s", "--silent", action = "store_true", default = False,
                      dest = "silent", help = "Suppress any logging statements below ERROR level")
    (options, args) = parser.parse_args()

    if options.verbose and options.silent:
        print "Conflicting options: silent and verbose. Exiting."
        return 1
    loggingLevel = logging.INFO
    if options.silent:
        loggingLevel = logging.ERROR
    if options.verbose:
        loggingLevel = logging.DEBUG
    logging.basicConfig(level = loggingLevel)
    logging.debug("Set verbose console output.")

    if not options.jsonfiles:
        logging.error("Need to provide jsonfile input option. Exiting.")
        return 1

    if not options.mode:
        logging.error("Need to provide use mode. Exiting.")
        return 1
    elif options.mode not in allowedModes:
        logging.error("Use mode needs to be in %s" % allowedModes)
        return 1

    injectNode = None
    injectSE = None
    if options.injectnode and options.injectse:
        injectNode = options.injectnode
        injectSE = options.injectse
    elif options.eos:
        injectNode = "T2_CH_CERN"
        injectSE = "srm-eoscms.cern.ch"
    elif options.castor:
        injectNode = "T0_CH_CERN_MSS"
        injectSE = "srm-cms.cern.ch"
    else:
        logging.error("Need to specific inject Node and SE. Exiting.")
        return 1

    fileDictList = []
    for jsonfile in options.jsonfiles:
        infile = open(jsonfile, 'r')
        data = json.loads(infile.read())
        for fileDict in data:
            fileDictList.append(fileDict)

    insertFiles(fileDictList, injectNode, injectSE, options.mode, options.commit)

    return

if __name__ == "__main__":
    sys.exit(main())
