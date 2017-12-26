const AWS = require('aws-sdk');
const async = require('async');
const { errors } = require('arsenal');
const Service = AWS.Service;

const GcpSigner = require('./GcpSigner');

AWS.apiLoader.services.gcp = {};
const GCP = Service.defineService('gcp', ['2017-11-01'], {
    getSignerClass() {
        return GcpSigner;
    },

    validateService() {
        if (!this.config.region) {
            this.config.region = 'us-east-1';
        }
    },

    putObjectTagging(params, callback) {
        if (!params.Tagging) {
            return callback(errors.MissingParameter);
        }
        const taggingParams = Object.assign({}, params);
        taggingParams.Metadata = params.Metadata || {};
        delete taggingParams.VersionId;
        delete taggingParams.Tagging;
        taggingParams.CopySource = `${params.Bucket}/${params.Key}`;
        taggingParams.MetadataDirective = 'REPLACE';
        if (params.Tagging.TagSet.length > 10) {
            return callback(errors.BadRequest
                .customizeDescription('Object tags cannot be greater than 10'));
        }
        const taggingDictionary = {};
        for (let i = 0; i < params.Tagging.TagSet.length; ++i) {
            const { Key: key, Value: value } = params.Tagging.TagSet[i];
            if (key.length > 128) {
                return callback(errors.InvalidTag
                    .customizeDescription(
                        'The TagKey you have provided is invalid'));
            }
            if (value.length > 256) {
                return callback(errors.InvalidTag
                    .customizeDescription(
                        'The TagValue you have provided is invalid'));
            }
            if (taggingDictionary[key]) {
                return callback(errors.InvalidTag
                    .customizeDescription(
                        'Cannot provide multiple Tags with the same key'));
            }
            taggingParams.Metadata[`aws-tag-${key}`] = value;
            taggingDictionary[key] = true;
        }
        return this.copyObject(taggingParams, callback);
    },

    getObjectTagging(params, callback) {
        const taggingParams = {
            Bucket: params.Bucket,
            Key: params.Key,
            VersionId: params.VersionId,
        };
        return async.waterfall([
            next => this.headObject(taggingParams, (err, res) => {
                if (err) {
                    return next(err);
                }
                return next(null, res);
            }),
            (resObj, next) => {
                const retObj = {
                    VersionId: resObj.VersionId,
                    TagSet: [],
                };
                Object.keys(resObj.Metadata).forEach(key => {
                    if (key.startsWith('aws-tag-')) {
                        retObj.TagSet.push({
                            Key: key.slice(8),
                            Value: resObj.Metadata[key],
                        });
                    }
                });
                return next(null, retObj);
            },
        ], (err, result) => {
            if (err) {
                return callback(err);
            }
            return callback(null, result);
        });
    },

    deleteObjectTagging(params, callback) {
        const taggingParams = {
            Bucket: params.Bucket,
            Key: params.Key,
            VersionId: params.VersionId,
        };
        return async.waterfall([
            next => this.headObject(taggingParams, (err, res) => {
                if (err) {
                    return next(err);
                }
                return next(null, res);
            }),
            (resObj, next) => {
                const retObj = {
                    VersionId: resObj.VersionId,
                    Metadata: {},
                };
                Object.keys(resObj.Metadata).forEach(key => {
                    if (!key.startsWith('aws-tag-')) {
                        retObj.Metadata[key] = resObj.Metadata[key];
                    }
                });
                return next(null, retObj);
            },
        ], (err, result) => {
            if (err) {
                return callback(err);
            }
            const taggingParams = {
                Bucket: params.Bucket,
                Key: params.Key,
                CopySource: `${params.Bucket}/${params.Key}`,
                MetadataDirective: 'REPLACE',
                Metadata: result.Metadata,
            };
            return this.copyObject(taggingParams, callback);
        });
    },

    putObjectCopy(params, callback) {
        return this.copyObject(params, callback);
    },

    upload(params, options, callback) {
        /* eslint-disable no-param-reassign */
        if (typeof options === 'function' && callback === undefined) {
            callback = options;
            options = null;
        }
        options = options || {};
        options = AWS.util.merge(options, { service: this, params });
        /* eslint-disable no-param-reassign */

        const uploader = new AWS.S3.ManagedUpload(options);
        if (typeof callback === 'function') uploader.send(callback);
        return uploader;
    },

    // Service API
    listBuckets(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: listBuckets not implemented'));
    },

    // Bucket APIs
    getBucketLocation(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getBucketLocation not implemented'));
    },

    deleteBucket(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteBucket not implemented'));
    },

    headBucket(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: headBucket not implemented'));
    },

    listObjects(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: listObjects not implemented'));
    },

    listObjectVersions(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: listObjecVersions not implemented'));
    },

    putBucket(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putBucket not implemented'));
    },

    getBucketAcl(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getBucketAcl not implemented'));
    },

    putBucketAcl(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putBucketAcl not implemented'));
    },

    putBucketWebsite(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putBucketWebsite not implemented'));
    },

    getBucketWebsite(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getBucketWebsite not implemented'));
    },

    deleteBucketWebsite(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteBucketWebsite not implemented'));
    },

    putBucketCors(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putBucketCors not implemented'));
    },

    getBucketCors(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getBucketCors not implemented'));
    },

    deleteBucketCors(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteBucketCors not implemented'));
    },

    // Object APIs
    deleteObjects(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteObjects not implemented'));
    },

    putObjectAcl(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putObjectAcl not implemented'));
    },

    getObjectAcl(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getObjectAcl not implemented'));
    },

    // Multipart upload
    abortMultipartUpload(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: abortMultipartUpload not implemented'));
    },

    createMultipartUpload(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: createMultipartUpload not implemented'));
    },

    completeMultipartUpload(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: completeMultipartUpload not implemented'));
    },

    uploadPart(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: uploadPart not implemented'));
    },

    uploadPartCopy(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: uploadPartCopy not implemented'));
    },

    listParts(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: listParts not implemented'));
    },
});

Object.defineProperty(AWS.apiLoader.services.gcp, '2017-11-01', {
    get: function get() {
        const model = require('./gcp-2017-11-01.api.json');
        return model;
    },
    enumerable: true,
    configurable: true,
});

module.exports = GCP;
