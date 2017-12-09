import os, sys
from pymongo import MongoClient
from optparse import OptionParser
from threading import Thread

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

def get_chains_by_vgene_and_aaSeqCDR3(db, vgene, aaSeqCDR3, study_id):
    """
        query chain collect for a list of chains of interest
        :param db: database handler
        :param aaSeqCDR3: aminino acid seequence of interest
        :param study_id: cancer study ID
        :type db: object
        :type aaSeqCDR3: str
        :type: study_id: str
        :return: a list of chains of interest
        :rtype: list
    """
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

def get_all_samples_by_study(db, study_id):
    """
        query sampe collect for a given study ID
        :param db: database handler
        :param study_id: cancer study ID
        :type db: object
        :type: study_id: str
        :return: a list of samples of interest
        :rtype: list
    """
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

def calculate_frequency(db, filename, chains, sample_size, study_id):
    """
        calculate the frequency of vgenes over of given sample size
        :param db: database handler
        :param filename: output file name include path
        :param chains: a list of chains of interest
        :param sample_size: size of samples
        :param study_id: id of cancer study
        :type db: object
        :type filename: str
        :type chains: list
        :type sample_size: int
        :type: study_id: str
        :return: None
    """
    with open(filename, 'w') as of:
        for chain_id in chains:
            v_gene = chain_id["_id"]["VGene"].strip()
            aa_seq = chain_id["_id"]["aaSeqCDR3"].strip()
            n_seq = chain_id["_id"]["nSeqCDR3"].strip()

            sample_query = get_chains_by_vgene_and_aaSeqCDR3(db, v_gene, aa_seq, study_id)
            sample_ids= list(set([ c["sample"]["_id"] for c in sample_query ]))
            print("sample IDs: %s" %str(sample_ids))

            count = len(sample_ids)
            if count > 1: print (count)
            one_dic = dict( _id=v_gene.split(",")[0],
                            aaSeqCDR3 = aa_seq,
                            sSeqCDR3 = n_seq,
                            count=str(count),
                            sample_size=str(sample_size),
                            sample_ids = sample_ids
                            )
            of.write("db.%s_frequency.insert(%s)\n"%(study_id, str(one_dic)))

def main():
    options = get_options()
    print (options)
    db = get_db(options.hostname, 27017)

    sample_query = get_all_samples_by_study(db, options.study_id)
    sample_size = len([ str(s) for s in sample_query])
    print ("sample size: %s"%sample_size)

    chain_query = get_unique_vgenes_by_study(db, options.study_id)
    chains = [ one_chain for one_chain in chain_query ]
    chain_size = len(chains)
    print (chain_size)
    chains_list = list(range(0, len(chains) + 1, 256))
    print (chains_list)
    idx0 = chains_list[0]
    for x in chains_list[1:]:
        print (idx0, x)
        t = Thread(target=calculate_frequency, args=[db, os.path.join(options.output,
                                                        "%s_%s.js"%(options.filename, idx0)),
                                                        chains[idx0:x], sample_size, options.study_id])
        t.start()
        idx0 = x
    print(idx0, chain_size-1)
    t = Thread(target=calculate_frequency, args=[db, os.path.join(options.output,
                                                                  "%s.js" % (options.filename)),
                                                 chains[idx0:chain_size-1], sample_size, options.study_id])
    t.start()

if __name__ == "__main__":
    main()