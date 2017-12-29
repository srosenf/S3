const replicationUtils = {
    requiredConfigProperties: [
        'Role',
        'Rules',
        'Status',
        'Prefix',
        'Destination',
        'Bucket',
    ],
    optionalConfigProperties: [
        'ID',
        'StorageClass',
    ],
    invalidRoleARNs: [
        '',
        '*:aws:iam::account-id:role/resource',
        'arn:*:iam::account-id:role/resource',
        'arn:aws:*::account-id:role/resource',
        'arn:aws:iam:*:account-id:role/resource',
        'arn:aws:iam::account-id:*',
        'arn:aws:iam::a:role',
        ',',
        // Check for comma-separated list > 2 in length.
        ',,',
        'arn:aws:iam::account-id:role/src,arn:aws:iam::account-id:role/dest',
    ],
    // Role value should be an Amazon Resource Name IAM user name format.
    validRoleARNs: [
        'arn:aws:iam::account-id:role',
        'arn:aws:iam::account-id:role/resource',
        'arn:aws:iam::account-id:role:resource',
        'arn:aws:iam::account-id:role*',
        'arn:aws:iam::ac:role',
        'arn:aws:iam::a c:role',
        'arn:aws:iam::*:role',

    ],
    invalidBucketARNs: [
        '',
        '*:aws:s3:::resource',
        'arn:*:s3:::resource',
        'arn:aws:*:::resource',
        'arn:aws:s3:*::resource',
        'arn:aws:s3::*:resource',
        'arn:aws:s3:::',
        'arn:aws:s3:::*',
        'arn:aws:s3:::invalidBucketName',
    ],
    validStatuses: [
        'Enabled',
        'Disabled',
    ],
    validStorageClasses: [
        'STANDARD',
        'STANDARD_IA',
        'REDUCED_REDUNDANCY',
    ],
    validMultipleStorageClasses: [
        'us-east-1,us-east-2',
    ],
};

module.exports = replicationUtils;
