const { waterfall } = require('async');
const { parseString } = require('xml2js');
const { errors } = require('arsenal');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');

/**
 * Format of xml request:

 <LifecycleConfiguration>
  <Rule>
    <ID>id1</ID>
    <Filter>
       <Prefix>logs/</Prefix>
    </Filter>
    <Status>Enabled</Status>
    <Expiration>
      <Days>365</Days>
    </Expiration>
  </Rule>
  <Rule>
    <ID>DeleteAfterBecomingNonCurrent</ID>
    <Filter>
       <Prefix>logs/</Prefix>
    </Filter>
    <Status>Enabled</Status>
    <NoncurrentVersionExpiration>
      <NoncurrentDays>1</NoncurrentDays>
    </NoncurrentVersionExpiration>
  </Rule>
</LifecycleConfiguration>
 */

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
            log.debug('request xml does not include at least one Rule');
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
            if (subFilter.Tag && (!subFilter.Tag[0].Key || !subFilter.Tag[0].Value)) {
                log.debug('Tag does not include both Key and Value');
                return cb(errors.MalformedXML);
            }
            if (!r.AbortIncompleteMultipartUpload && !r.Expiration && !r.NoncurrentVersionExpiration) {
                log.debug('Rule xml does not include valid action');
                return cb(errors.MalformedXML);
            }
            if (r.AbortIncompleteMultipartUpload && !r.AbortIncompleteMultipartUpload[0].DaysAfterInitiation) {
                log.debug('AbortIncompleteMultipartUpload action does not include DaysAfterInitiation');
                return cb(errors.MalformedXML);
            }
            if (r.Expiration && (!r.Expiration[0].Date && !r.Expiration[0].Days)) {
                log.debug('Expiration action does not include Date or Days');
                return cb(errors.MalformedXML);
            }
            if (r.NoncurrentVersionExpiration && !r.NoncurrentVersionExpiration[0].NoncurrentDays) {
                log.debug('NoncurrentVersionExpiration does not include NoncurrentDays');
                return cb(errors.MalformedXML);
            }
        });
        return process.nextTick(() => cb(null, rulesArray));
    });
}

/**
 * Bucket Put Versioning - Create or update bucket Versioning
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
        next => _parseXML(request, log, next),
        (rulesArray, next) => metadataValidateBucket(metadataValParams, log,
            (err, bucket) => {
                if (err) {
                    return next(err, bucket);
                }
                const lifecycleConfiguration = {};
                // the configuration has been checked before
                return next(null, bucket, lifecycleConfiguration);
            }),
        (bucket, versioningConfiguration, next) => {
            bucket.setVersioningConfiguration(versioningConfiguration);
            // TODO all metadata updates of bucket should be using CAS
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
