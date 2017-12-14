import os
import sys
import re
from bson import ObjectId
from pymongo import MongoClient
from optparse import OptionParser
from threading import Thread
from utils import get_logger


def get_options():
    """
        collect input from the user
        :return: list of options
        :rtype: list
    """
    parser = OptionParser()
    parser.add_option('-n', '--hostname',
                      dest="hostname",
                      type="string",
                      help="mongodb server name"
                      )
    parser.add_option('-o', '--output',
                      dest="output",
                      type="string",
                      help="output directory"
                      )
    parser.add_option('-f', '--file',
                      dest="filename",
                      default="default.out",
                      type="string",
                      help="filename without path"
                      )
    parser.add_option('-u', '--user',
                      dest="user",
                      type="string",
                      help="user name if authentication truned on"
                      )
    parser.add_option('-p', '--password',
                      dest="password",
                      type="string",
                      help="password of the user"
                      )
    parser.add_option('-s', '--study_id',
                      dest="study_id",
                      type="string",
                      help="id of study e.g TARGET"
                      )
    parser.add_option('-c', '--cancer_type_id',
                      dest="cancer_type_id",
                      type="string",
                      default=None,
                      help="id of cancer type e.g NBL"
                      )
    (options, args) = parser.parse_args()
    return options

def get_db(hostname, port=None, user=None, password=None):
    """
        create a database handler object
        :param hostname: mongodb hostname
        :param port: port of mongodb listen
        :param user: login ID of the user
        :param password: password of the user
        :type hostname: str
        :type port: int
        :type user: str
        :type password: str
        :return: database handler
        :rtype: object
    """
    port = 27017 if not port else port
    client = MongoClient("%s:%s"%(hostname, port))
    db = client.immds
    if user and password:
        db.authenticate(user, password)
    return db

def get_unique_vgenes_by_study(db, study_id):
    """
        query chain collect for a list of distinct vgenes
        :param db: database handler
        :param study_id: id of study e.g TARGET
        :type db: object
        :type study_id: str
        :return: database cursor
        :rtype: object
    """
    pipeline = [{ "$group": {
        "_id": {"VGene": "$VGene", "aaSeqCDR3": "$aaSeqCDR3", "nSeqCDR3": "$nSeqCDR3"}
    }}]
    if study_id == "TLML":
        return db.TLML.aggregate(pipeline)
    elif study_id == "TARGET":
        return db.TARGET.aggregate(pipeline)
    else:
        return None

def get_chains_by_vgene_and_aaSeqCDR3(db, vgene, aaSeqCDR3, study_id, cancer_type_id=None):
    """
        query chain collect for a list of chains of interest
        :param db: database handler
        :param aaSeqCDR3: aminino acid seequence of interest
        :param study_id: cancer study ID
        :param cancer_type_id: cancer type ID eg NBL
        :type db: object
        :type aaSeqCDR3: str
        :type study_id: str
        :type cancer_type_id: str
        :return: a list of chains of interest
        :rtype: list
    """
    if cancer_type_id:
        pipeline = [
            {'$lookup':
                {
                    'from': "assay",
                    'localField': "assay_id",
                    'foreignField': "_id",
                    'as': "assay"
                }
            },
            {'$unwind': "$assay"},
            {'$lookup':
                {
                    'from': "sample",
                    'localField': "assay.sample_id",
                    'foreignField': "_id",
                    'as': "sample"
                }
            },
            {'$unwind': "$sample"},
            {'$lookup':
                {
                    'from': "patient",
                    'localField': "sample.patient_id",
                    'foreignField': "_id",
                    'as': "patient"
                }
            },
            {'$unwind': "$patient"},
            {
                '$match': {"VGene": vgene, "aaSeqCDR3": aaSeqCDR3, "sample.cancer_type_id": cancer_type_id}
            }]
    else:
        pipeline = [
            {'$lookup':
                {
                    'from': "assay",
                    'localField': "assay_id",
                    'foreignField': "_id",
                    'as': "assay"
                }
            },
            {'$unwind': "$assay"},
            {'$lookup':
                {
                    'from': "sample",
                    'localField': "assay.sample_id",
                    'foreignField': "_id",
                    'as': "sample"
                }
            },
            {'$unwind': "$sample"},
            {'$lookup':
                {
                    'from': "patient",
                    'localField': "sample.patient_id",
                    'foreignField': "_id",
                    'as': "patient"
                }
            },
            {'$unwind': "$patient"},
            {
                '$match': {"VGene": vgene, "aaSeqCDR3": aaSeqCDR3}
            }]
    if study_id == "TARGET":
        return db.TARGET.aggregate(pipeline)
    elif study_id == "TLML":
        return  db.TLML.aggregate(pipeline)
    else:
        return None

def get_all_samples_by_study(db, study_id, cancer_type_id=None):
    """
        query sampe collect for a given study ID
        :param db: database handler
        :param study_id: cancer study ID
        :param cancer_type_id: ID of cancer type eg NBL
        :type db: object
        :type: study_id: str
        :type: cancer_study_id: str
        :return: a list of samples of interest
        :rtype: list
    """
    if cancer_type_id:
        return db.sample.aggregate([
        {'$lookup':
            {
                'from': "patient",
                'localField': "patient_id",
                'foreignField': "_id",
                'as': "patient"
            }
        },
        {'$unwind': "$patient"},
        {
            '$match': {"patient.study_id": study_id, "cancer_type_id": cancer_type_id}
        }])
    else:
        return db.sample.aggregate([
        {'$lookup':
            {
                'from': "patient",
                'localField': "patient_id",
                'foreignField': "_id",
                'as': "patient"
            }
        },
        {'$unwind': "$patient"},
        {
            '$match': {"patient.study_id": study_id}
        }])

def add_vgene_frequency(db, arg_dict, study_id):
    """
        add new row to frequency db table
        :param db: database handler
        :parm arg_dict frequency dictionary
        :param study_id: id of study e.g TARGET
        :type db: object
        :type arg_dict: dictionary
        :type study_id: str
        :return: row ID
        :rtype: str
    """
    if study_id == "TLML":
        result = db.TLML_frequency.insert_one(arg_dict)
        return result.inserted_id
    elif study_id == 'TRAGET':
        result = db.TARGET_frequency.insert_one(arg_dict)
        result.inserted_id
    else:
        return None

def calculate_frequency(db, logger, chains, sample_size, study_id, cancer_type_id):
    """
        calculate the frequency of vgenes over of given sample size
        :param db: database handler
        :param logger: logger object
        :param chains: a list of chains of interest
        :param sample_size: size of samples
        :param study_id: id of cancer study
        :type db: object
        :type logger: object
        :type chains: list
        :type sample_size: int
        :type: study_id: str
        :return: None
    """
    for chain_id in chains:
        try:
            v_gene = chain_id["_id"]["VGene"].strip()
            aa_seq = chain_id["_id"]["aaSeqCDR3"].strip()
            n_seq = chain_id["_id"]["nSeqCDR3"].strip()

            sample_query = get_chains_by_vgene_and_aaSeqCDR3(db, v_gene, aa_seq, study_id, cancer_type_id)
            sample_ids= list(set([ c["sample"]["_id"] for c in sample_query ]))
            logger.info("sample IDs: %s" %str(sample_ids))

            count = len(sample_ids)
            one_dic = dict( _id= str(ObjectId()),
                            VGene = v_gene.split(",")[0],
                            aaSeqCDR3 = aa_seq,
                            nSeqCDR3 = n_seq,
                            count=str(count),
                            sample_size=str(sample_size),
                            sample_ids = sample_ids
                            )
            row_id = add_vgene_frequency(db,one_dic, "TLML")
            logger.info(str(one_dic))
        except Exception as e:
            logger.error("error occurs while calculating frequency: %s" %e.message)
            continue

def get_vgene_frequency(db, logger, study_id, VGene, count=None):
    """
        retrieve the frequency for a VGene of interst
        :param db: database handler
        :param logger: logger object
        :param study_id: study ID eg. TARGET
        :param VGene: VGene of interest
        :param count: number of counts
        :type db: object
        :type logger: object
        :type study_id: str
        :type VGene: str
        :type count: int
        :return counts of VGene of interest
        :rtype list
    """
    try:
        regx = re.compile("%s.*"%VGene, re.IGNORECASE)
        if study_id == "TLML":
            if count:
                return db.TLML_frequency.find({"VGene": regx, "count":{'$gte': count}})
            else:
                return db.TLML_frequency.find({"VGene": regx})
        elif study_id == "TRAGET":
            if count:
                return db.TRAGET_frequency.find({"VGene": regx, "count": {'$gte': count}})
            else:
                return db.TRAGET_frequency.find({"VGene": regx})
    except Exception as e:
        logger.error("failed to get frequency for %s: %s" %(VGene, e.message))

def main():
    options = get_options()
    logger = get_logger("immds", os.path.join(options.output, 'immds_freq.log'))
    logger.info(str(options))

    try:
        db = get_db(options.hostname, 27017)
    except Exception as e:
        logger.error("cannot connect to mongodb: %s" %e.message)

    try:
        sample_query = get_all_samples_by_study(db, options.study_id, options.cancer_type_id)
        sample_size = len([str(s) for s in sample_query])
        logger.info("sample size: %s" % sample_size)
    except Exception as e:
        logger.error("cannot retrieve samples for %s: %s"%(options.study_id, e.message))

    try:
        chain_query = get_unique_vgenes_by_study(db, options.study_id)
        chains = [ one_chain for one_chain in chain_query ]
        chain_size = len(chains)
        logger.info("total records: %s" %chain_size)
        chains_list = list(range(0, len(chains) + 1, 256))
    except Exception as e:
        logger.error("can not retrieve set of vgenes for %s: %s" %(options.study_id, e.message))

    idx0 = chains_list[0]
    for x in chains_list[1:]:
        t = Thread(target=calculate_frequency, args=[db, logger, chains[idx0:x], sample_size,
                                                     options.study_id, options.cancer_type_id])
        t.start()
        idx0 = x
    t = Thread(target=calculate_frequency, args=[db, logger, chains[idx0:chain_size], sample_size,
                                                 options.study_id, options.cancer_type_id])
    t.start()

if __name__ == "__main__":
    main()