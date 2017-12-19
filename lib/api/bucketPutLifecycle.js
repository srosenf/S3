const { waterfall } = require('async');
const { errors } = require('arsenal');

const parseXML = require('../utilities/parseXML');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');

function _parseXML(request, log, cb) {
    if (request.post === '') {
        log.debug('request xml is missing');
        return cb(errors.MalformedXML);
    }
    return parseString(request.post, (err, result) => {
        if (err) {
            log.debug('request xml is malformed');
            return cb(errors.MalformedXML);
        }
        const lifecycleConf = result.LifecycleConfiguration;
        if (!lifecycleConf) {
            log.debug('request xml does not include LifecycleConfiguration');
            return cb(errors.MalformedXML);
        }
        const rulesArray = lifecycleConf.Rule;
        if (!rulesArray || rulesArray.length === 0) {
            log.debug('request xml does not include at least one rule');
            return cb(errors.MalformedXML);
        } else if (rulesArray.length > 1000) {
            log.debug('request xml includes over max limit of 1000 rules');
            return cb(errors.MalformedXML);
        }
        rulesArray.forEach(r => {
            if (r.Transition || r.NoncurrentVersionTransition) {
                log.debug('Transition lifecycle action not yet implemented');
                return cb(errors.NotImplemented);
            }
            if (!r.Filter || !r.Status) {
                log.debug('Rule xml does not include Filter or Status');
                return cb(errors.MalformedXML);
            }
            const subFilter = r.Filter[0].And || r.Filter[0];
            if (!subFilter.Prefix && !subFilter.Tag) {
                log.debug('Filter or And does not include Prefix or Tag');
                return cb(errors.MalformedXML);
            }
            if (subFilter.Tag
                && (!subFilter.Tag[0].Key || !subFilter.Tag[0].Value)) {
                log.debug('Tag does not include both Key and Value');
                return cb(errors.MalformedXML);
            }
            if (r.ID && r.ID[0].length > 255) {
                log.debug('Rule ID is greater than 255 characters long');
                return cb(errors.MalformedXML);
            }
            if (!r.AbortIncompleteMultipartUpload && !r.Expiration
                && !r.NoncurrentVersionExpiration) {
                log.debug('Rule does not include valid action');
                return cb(errors.MalformedXML);
            }
            if (r.AbortIncompleteMultipartUpload) {
                if (subFilter.Tag) {
                    log.debug('Tag-based filter cannot be used with ' +
                        'AbortIncompleteMultipartUpload action');
                    return cb(errors.MalformedXML);
                }
                const subAbort = r.AbortIncompleteMultipartUpload[0];
                if (!subAbort.DaysAfterInitiation) {
                    log.debug('AbortIncompleteMultipartUpload action does ' +
                        'not include DaysAfterInitiation');
                    return cb(errors.MalformedXML);
                }
                if (parseInt(subAbort.DaysAfterInitiation[0], 10) < 1) {
                    log.debug('DaysAfterInitiation is not a positive integer');
                }
            }
            if (r.Expiration) {
                const subExp = r.Expiration[0];
                if (!subExp.Date && !subExp.Days) {
                    log.debug('Expiration action does not include ' +
                        'Date or Days');
                    return cb(errors.MalformedXML);
                }
                if (subExp.Date) {
                    // verify ISO 8601 format
                }
                if (subExp.Days && parseInt(subExp.Days[0], 10) < 1) {
                    log.debug('Expiration days is not a positive integer');
                    return cb(errors.MalformedXML);
                }
            }
            if (r.NoncurrentVersionExpiration) {
                const subNVExp = r.NoncurrentVersionExpiration[0];
                if (!subNVExp.NoncurrentDays) {
                    log.debug('NoncurrentVersionExpiration action does not ' +
                        'include NoncurrentDays');
                    return cb(errors.MalformedXML);
                }
                if (parseInt(subNVExp.NoncurrentDays[0], 10) < 1) {
                    log.debug('NoncurrentDays is not a positive integer');
                    return cb(errors.MalformedXML);
                }
            }
        });
        return process.nextTick(() => cb(null, rulesArray));
    });
}

/**
 * Bucket Put Versioning - Create or update bucket lifecycle configuration
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */

function bucketPutLifecycle(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutLifecycle' });

    const bucketName = request.bucketName;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketOwnerAction',
    };
    return waterfall([
        next => parseXML(request, log, next),
        (rulesArray, next) => metadataValidateBucket(metadataValParams, log,
            (err, bucket) => {
                if (err) {
                    return next(err, bucket);
                }
                const lifecycleConfiguration = {};
                return next(null, bucket, lifecycleConfiguration);
            }),
        (bucket, lifecycleConfiguration, next) => {
            bucket.setLifecycleConfiguration(lifecycleConfiguration);
            metadata.updateBucket(bucket.getName(), bucket, log, err =>
                next(err, bucket));
        },
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'bucketPutVersioning' });
        } else {
            pushMetric('putBucketVersioning', log, {
                authInfo,
                bucket: bucketName,
            });
        }
        return callback(err, corsHeaders);
    });
}

module.exports = bucketPutLifecycle;
