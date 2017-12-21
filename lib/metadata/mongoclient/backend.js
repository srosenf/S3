/* 
 * we assume good default settings of write concern is good for all
 * bulk writes. Note that bulk writes are not transactions but ordered
 * writes. They may fail in between. To some extend those situations
 * may generate orphans but not alter the proper conduct of operations
 * (what he user wants and what we acknowledge to the user).
 *
 * Orphan situations may be recovered by the Lifecycle.
 *
 * We use proper atomic operations when needed.
 */

const util = require('util');
const arsenal = require('arsenal');

const logger = require('../../utilities/logger');

const constants = require('../../../constants');
const { config } = require('../../Config');

const errors = arsenal.errors;
const versioning = arsenal.versioning;
const BucketInfo = arsenal.models.BucketInfo;

const MongoClient = require('mongodb').MongoClient;

const Version = versioning.Version;
const genVID = versioning.VersionID.generateVersionId;

const MongoReadStream = require('./readStream');

const METASTORE = '__metastore';

const crypto = require("crypto");

let uidCounter = 0;

const VID_SEP = versioning.VersioningConstants.VersionId.Separator;

function generateVersionId() {
    // generate a unique number for each member of the nodejs cluster
    return genVID(`${process.pid}.${uidCounter++}`,
		  config.replicationGroupId);
}

function formatVersionKey(key, versionId) {
    return `${key}${VID_SEP}${versionId}`;
}

function inc(str) {
    return str ? (str.slice(0, str.length - 1) +
            String.fromCharCode(str.charCodeAt(str.length - 1) + 1)) : str;
}

const VID_SEPPLUS = inc(VID_SEP);

function generatePHDVersion(versionId) {
    return { "isPHD": true, "versionId": versionId };
}

class MongoClientInterface {
    constructor() {
	const mongoUrl =
	      `mongodb://${config.mongodb.host}:${config.mongodb.port}`;

	this.client = null;
	this.db = null;
	console.log('connecting to', mongoUrl);
        MongoClient.connect(mongoUrl, (err, client) => {
	    if (err) {
		throw (errors.InternalError);
	    }
	    console.log('***CONNECTED TO MONGODB***');
	    this.client = client;
	    this.db = client.db(config.mongodb.database);
	    this.usersBucketHack();
	});
    }

    usersBucketHack() {
	/* Since the bucket creation API is expecting the usersBucket
           to have attributes, we pre-create the usersBucket
           attributes here (see bucketCreation.js line 36)*/
        const usersBucketAttr = new BucketInfo(constants.usersBucket,
            'admin', 'admin', new Date().toJSON(),
            BucketInfo.currentModelVersion());
	this.createBucket(
            constants.usersBucket,
            usersBucketAttr, {}, err => {
                if (err) {
                    console.log('error writing usersBucket ' +
                                'attributes to metastore',
                                { error: err });
                    throw (errors.InternalError);
                }
            });
    }
    
    getCollection(name) {
	/* mongo has a problem with .. in collection names */
	if (name === constants.usersBucket)
	    name = "users__bucket";
	return this.db.collection(name);
    }
    
    createBucket(bucketName, bucketMD, log, cb) {
	console.log('mb +', bucketName);
	var m = this.getCollection(METASTORE);
	m.update({
	    _id: bucketName
	}, {
	    _id: bucketName,
	    value: bucketMD
	}, {
	    upsert: true
	}, () => {
	    return cb()
	});
    }

    getBucketAttributes(bucketName, log, cb) {
	console.log('gba +', bucketName);
	var m = this.getCollection(METASTORE);
	m.findOne({
	    _id: bucketName
	}, (err, doc) => {
	    console.log(err, doc);
	    if (err)
		return cb(errors.InternalError);
	    if (!doc) {
		return cb(errors.NoSuchBucket);
	    }
	    return cb(null, doc.value);
	});
    }

    getBucketAndObject(bucketName, objName, params, log, cb) {
	console.log('gboa +', bucketName, objName);
	this.getBucketAttributes(bucketName, log, (err, bucket) => {
	    if (err) {
		return cb(err);
	    }
	    this.getObject(bucketName, objName, params, log, (err, obj) => {
		if (err) {
		    if (err === errors.NoSuchKey) {
			return cb(null,
				  { bucket:
				    BucketInfo.fromObj(bucket).serialize()
				  });
		    } else {
			return cb(err);
		    }
		}
		return cb(null, {
                    bucket: BucketInfo.fromObj(bucket).serialize(),
                    obj: JSON.stringify(obj)
		});
	    });
	});
    }

    putBucketAttributes(bucketName, bucketMD, log, cb) {
	console.log('pba +', bucketName);
	var m = this.getCollection(METASTORE);
	m.update({
	    _id: bucketName
	}, {
	    _id: bucketName,
	    value: bucketMD
	}, {
	    upsert: true
	}, () => {
	    return cb()
	});
    }

    /**
     * Delete bucket from metastore
     */
    deleteBucketStep2(bucketName, log, cb) {
	console.log('dbs2 +', bucketName);
	var m = this.getCollection(METASTORE);
	m.findOneAndDelete({
	    _id: bucketName
	}, (err, result) => {
	    console.log(err, result);
	    if (err)
		return cb(errors.InternalError);
	    if (result.ok !== 1) {
		return cb(errors.InternalError);
	    }
	    return cb(null);
	});
    }

    /**
     * Check if the bucket is empty then process to step 2. Checking
     * the count is already done by the upper layer. We don't need to be 
     * atomic because the call is protected by a delete_pending flag 
     * in the upper layer.
     * 2 cases here:
     * 1) the collection may not yet exist (or being already dropped
     * by a previous call) 
     * 2) the collection may exist.  
     */
    deleteBucket(bucketName, log, cb) {
	console.log('db +', bucketName);
	var c = this.getCollection(bucketName);
	c.drop(err => {
	    console.log('*** DROP ***', err);
	    if (err) {
		if (err.codeName === 'NamespaceNotFound') {
		    return this.deleteBucketStep2(bucketName, log, cb);
		}
		return cb(errors.InternalError);
	    }
	    return this.deleteBucketStep2(bucketName, log, cb);
	});
    }

    /**
     * In this case we generate a versionId and 
     * sequentially create the object THEN update the master
     */
    putObjectVerCase1(c, bucketName, objName, objVal, params, log, cb) {
	const versionId = generateVersionId();
        objVal.versionId = versionId;
	const vObjName = formatVersionKey(objName, versionId);
	c.bulkWrite([{
	    updateOne: {
		filter: {
		    _id: vObjName,
		},
		update: {
		    _id: vObjName, value: objVal
		},
		upsert: true
	    }
	}, {
	    updateOne: {
		filter: {
		    _id: objName,
		},
		update: {
		    _id: objName, value: objVal
		},
		upsert: true
	    }
	}], {
	    ordered: 1
	}, () => {
	    return cb(null, `{"versionId": "${versionId}"}`);
	});
    }

    /**
     * Case used when versioning has been disabled after objects
     * have been created with versions
     */
    putObjectVerCase2(c, bucketName, objName, objVal, params, log, cb) {
	const versionId = generateVersionId();
        objVal.versionId = versionId;
	c.update({
	    _id: objName
	}, {
	    _id: objName,
	    value: objVal
	}, {
	    upsert: true
	}, () => {
	    return cb(null, `{"versionId": "${objVal.versionId}"}`);
	});
    }

    /**
     * In this case the aller provides a versionId. This function will
     * sequentially update the object with given versionId THEN the
     * master iff the provided versionId matches the one of the master
     */
    putObjectVerCase3(c, bucketName, objName, objVal, params, log, cb) {
	objVal.versionId = params.versionId;
	vObjName = formatVersionKey(objName, params.versionId);
	c.bulkWrite([{
	    updateOne: {
		filter: {
		    _id: vObjName,
		},
		update: {
		    _id: vObjName, value: objVal
		},
		upsert: true
	    }
	}, {
	    updateOne: {
		filter: {
		    _id: objName,
		    "value.versionId": params.versionId
		},
		update: {
		    _id: objName, value: objVal
		},
		upsert: true
	    }
	}], {
	    ordered: 1
	}, () => {
	    return cb(null, `{"versionId": "${objVal.versionId}"}`);
	});
    }

    /**
     * Put object when versioning is not enabled
     */
    putObjectNoVer(c, bucketName, objName, objVal, params, log, cb) {
	c.update({
	    _id: objName
	}, {
	    _id: objName,
	    value: objVal
	}, {
	    upsert: true
	}, () => {
	    return cb()
	});
    }
    
    putObject(bucketName, objName, objVal, params, log, cb) {
	console.log('po +', bucketName, objName);
	var c = this.getCollection(bucketName);
	if (params && params.versioning) {
	    return this.putObjectVerCase1(c, bucketName, objName, objVal,
					  params, log, cb);
        } else if (params && params.versionId === '') {
	    return this.putObjectVerCase2(c, bucketName, objName, objVal,
					  params, log, cb);
        } else if (params && params.versionId) {
	    return this.putObjectVerCase3(c, bucketName, objName, objVal,
					  params, log, cb);
        } else {
	    return this.putObjectNoVer(c, bucketName, objName, objVal,
				       params, log, cb);
	}
    }

    getObject(bucketName, objName, params, log, cb) {
	console.log('go +', bucketName, objName);
	var c = this.getCollection(bucketName);
	if (params && params.versionId) {
            objName = formatVersionKey(objName, params.versionId);
	    console.log('gov +', bucketName, objName);
        }
	c.findOne({
	    _id: objName
	}, (err, doc) => {
	    console.log(err, doc);
	    if (err)
		return cb(errors.InternalError);
	    if (!doc) {
		return cb(errors.NoSuchKey);
	    }
	    if (doc.value.isPHD) {
		console.log('xxx');
		return this.getObjectByListing(c, objName,
					       log, cb);
	    } else {
		console.log('yyy');
		return cb(null, doc.value);
	    }
	});
    }
    
    /**
     * This function is called when the master is a PHD
     */
    getObjectByListing(c, objName, log, cb) {
	console.log('gobl', objName);
	c.find({
	    _id: {
		$gt: objName,
		$lt: `${objName}${VID_SEPPLUS}`
	    }
	}).
	    sort({_id: -1}).
	    limit(2).
	    toArray(
		(err, keys) => {
		    console.log('*** RESULT ***', err, keys);
		    if (err)
			return cb(errors.InternalError);
		    if (keys.length === 0)
			return cb(errors.NoSuchKey);
		    this.repair(c, objName, keys[0].value, log, err => {
			if (err)
			    return (errors.InternalError);
			return cb(null, keys[0].value);
		    });
		});
    }

    /* 
     * repair the master with a new value
     *
     * @param {object} collection
     * @param {string} objName - the master name
     * @param {object} objVal - the new value
     * @param {object} log -
     * @param {function} cb - cb(err)
     */
    repair(c, objName, objVal, log, cb) {
	console.log('*** Repair ***');
	c.findOneAndReplace({
	    _id: objName
	}, {
	    _id: objName,
	    value: objVal
	}, {
	    upsert: true
	}, (err, result) => {
	    return cb(null);
	});
    }
    
    asyncRepair(c, objName, log) {
	console.log('*** Repair PHD ***');
	this.getObjectByListing(c, objName, log, err => {
	    if (err) {
		console.log('error repairing', err);
		return ;
	    }
	    console.log('repair success');
	});
    }
    
    /**
     * Delete object when versioning is enabled and the version is
     * master (or if the master is already a PHD see thereafter). In
     * this case we sequentially update the master with a PHD flag
     * (placeholder) and a unique non-existing version THEN we delete
     * the specified versioned object. A repair process will occur
     * later.
     */
    deleteObjectVerMaster(c, bucketName, objName, params, log, cb) {
	console.log('dovm +', bucketName, objName);
	const vObjName = formatVersionKey(objName, params.versionId);
	const _vid = generateVersionId();
	const mst = generatePHDVersion(_vid);
	c.bulkWrite([{
	    deleteOne: {
		filter: {
		    _id: vObjName,
		}
	    }
	}, {
	    updateOne: {
		filter: {
		    _id: objName,
		},
		update: {
		    _id: objName, value: mst
		},
		upsert: true
	    }
	}], {
	    ordered: 1
	}, () => {
	    setTimeout(() => {
		this.asyncRepair(c, objName, log);
	    }, 15000);
	    return cb(null);
	});
    }

    /**
     * Delete object when versioning is enabled and the version is
     * not master. It reduces to a simple atomic delete
     */
    deleteObjectVerNotMaster(c, bucketName, objName, params, log, cb) {
	console.log('dovnm +', bucketName, objName);
	const vObjName = formatVersionKey(objName, params.versionId);
	c.findOneAndDelete({
	    _id: vObjName
	}, (err, result) => {
	    console.log(err, result);
	    if (err)
		return cb(errors.InternalError);
	    if (result.ok !== 1) {
		return cb(errors.InternalError);
	    }
	    return cb(null);
	});
    }
    
    /**
     * Delete object when versioning is enabled. We first find
     * the master, then check if it matches the master versionId
     */
    deleteObjectVer(c, bucketName, objName, params, log, cb) {
	console.log('dov +', bucketName, objName, params.versionId);
	c.findOne({
	    _id: objName
	}, (err, mst) => {
	    console.log(err, mst);
	    if (err)
		return cb(errors.InternalError);
	    if (!mst) {
		return cb(errors.NoSuchKey);
	    }
	    if (mst.value.isPHD ||
		mst.value.versionId === params.versionId) {
		return this.deleteObjectVerMaster(c, bucketName, objName,
						  params, log, cb);
	    } else {
		return this.deleteObjectVerNotMaster(c, bucketName, objName,
						     params, log, cb);
	    }
	});
    }

    /**
     * Atomically delete an object when versioning is not enabled
     */
    deleteObjectNoVer(c, bucketName, objName, params, log, cb) {
	console.log('donv +', bucketName, objName);
	c.findOneAndDelete({
	    _id: objName
	}, (err, result) => {
	    console.log(err, result);
	    if (err)
		return cb(errors.InternalError);
	    if (result.ok !== 1) {
		return cb(errors.InternalError);
	    }
	    return cb(null);
	});
    }
    
    deleteObject(bucketName, objName, params, log, cb) {
	console.log('do +', bucketName, objName);
	var c = this.getCollection(bucketName);
	if (params && params.versionId) {
	    return this.deleteObjectVer(c, bucketName, objName,
					params, log, cb);
	} else {
	    return this.deleteObjectNoVer(c, bucketName, objName,
					  params, log, cb);
	}
    }
    
    internalListObject(bucketName, params, log, cb) {
        const extName = params.listingType;
        const extension = new arsenal.algorithms.list[extName](params, log);
        const requestParams = extension.genMDParams();
	var c = this.getCollection(bucketName);
        let cbDone = false;
        let stream = new MongoReadStream(c, requestParams);
        stream
            .on('data', e => {
                if (extension.filter(e) < 0) {
                    stream.emit('end');
                    stream.destroy();
                }
            })
            .on('error', err => {
                if (!cbDone) {
                    cbDone = true;
                    const logObj = {
                        rawError: err,
                        error: err.message,
                        errorStack: err.stack,
                    };
                    log.error('error listing objects', logObj);
                    cb(errors.InternalError);
                }
            })
            .on('end', () => {
                if (!cbDone) {
                    cbDone = true;
                    const data = extension.result();
                    cb(null, data);
                }
            });
        return undefined;
    }

    listObject(bucketName, params, log, cb) {
	console.log('lo +', bucketName);
        return this.internalListObject(bucketName, params, log, cb);
    }

    listMultipartUploads(bucketName, params, log, cb) {
	console.log('lmpu +', bucketName);
        return this.internalListObject(bucketName, params, log, cb);
    }
}

module.exports = MongoClientInterface;
