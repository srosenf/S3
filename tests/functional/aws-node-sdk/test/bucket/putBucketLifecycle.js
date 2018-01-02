const assert = require('assert');
const { errors } = require('arsenal');
const { S3 } = require('aws-sdk');

const getConfig = require('../support/config');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'lifecycleputtestbucket';
const lifecycleConfig = {
    Rules: [
        {
            ID: 'test-id',
            Prefix: '',
            Status: 'Enabled',
            Expiration: {
                Days: 1,
            },
        },
    ],
};

// Check for the expected error response code and status code.
function assertError(err, expectedErr) {
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        assert.strictEqual(err.code, expectedErr, 'incorrect error response ' +
            `code: should be '${expectedErr}' but got '${err.code}'`);
        assert.strictEqual(err.statusCode, errors[expectedErr].code,
            'incorrect error status code: should be 400 but got ' +
            `'${err.statusCode}'`);
    }
}

describe.only('aws-sdk test put bucket lifecycle', () => {
    let s3;
    let otherAccountS3;
    const lifecycleParams = {
        Bucket: bucket,
        LifecycleConfiguration: lifecycleConfig,
    };

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return done();
    });

    it('should return NoSuchBucket error if bucket does not exist', done =>
    s3.putBucketLifecycleConfiguration(lifecycleParams, err => {
        assertError(err, 'NoSuchBucket');
        return done();
    }));

    describe('config rules', () => {
        beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

        afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

        it('should return AccessDenied if user is not bucket owner', done =>
        otherAccountS3.putBucketLifecycle(lifecycleParams, err => {
            assertError(err, 'AccessDenied');
            return done();
        }));
    });
});
