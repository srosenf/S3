const assert = require('assert');
const collectResponseHeaders =
    require('../../../lib/utilities/collectResponseHeaders');

describe('Middleware: Collect Response Headers', () => {
    it('should be able to set replication status when config is set', () => {
        const objectMD = { replicationInfo: { status: 'REPLICA' } };
        const headers = collectResponseHeaders(objectMD);
        assert.deepStrictEqual(headers['x-amz-replication-status'], 'REPLICA');
    });

    it('should set the global replication status to PENDING if all backend ' +
    'statuses are PENDING or COMPLETED', () => {
        const objectMD = {
            replicationInfo: {
                status: '',
                backends: [
                    {
                        site: 'a',
                        status: 'PENDING',
                    },
                    {
                        site: 'b',
                        status: 'COMPLETED',
                    },
                ],
            },
        };
        const headers = collectResponseHeaders(objectMD);
        assert.strictEqual(headers['x-amz-replication-status'], 'PENDING');
        assert.strictEqual(headers['x-amz-a-replication-status'], 'PENDING');
        assert.strictEqual(headers['x-amz-b-replication-status'], 'COMPLETED');
    });

    it('should set the global replication status to COMPLETED if all backend ' +
    'statuses are COMPLETED', () => {
        const objectMD = {
            replicationInfo: {
                status: '',
                backends: [
                    {
                        site: 'a',
                        status: 'COMPLETED',
                    },
                    {
                        site: 'b',
                        status: 'COMPLETED',
                    },
                ],
            },
        };
        const headers = collectResponseHeaders(objectMD);
        assert.strictEqual(headers['x-amz-replication-status'], 'COMPLETED');
        assert.strictEqual(headers['x-amz-a-replication-status'], 'COMPLETED');
        assert.strictEqual(headers['x-amz-b-replication-status'], 'COMPLETED');
    });

    it('should set the global replication status to FAILED if any backend ' +
    'status is FAILED', () => {
        const objectMD = {
            replicationInfo: {
                status: '',
                backends: [
                    {
                        site: 'a',
                        status: 'PENDING',
                    },
                    {
                        site: 'b',
                        status: 'FAILED',
                    },
                    {
                        site: 'c',
                        status: 'COMPLETED',
                    },
                ],
            },
        };
        const headers = collectResponseHeaders(objectMD);
        assert.strictEqual(headers['x-amz-replication-status'], 'FAILED');
        assert.strictEqual(headers['x-amz-a-replication-status'], 'PENDING');
        assert.strictEqual(headers['x-amz-b-replication-status'], 'FAILED');
        assert.strictEqual(headers['x-amz-c-replication-status'], 'COMPLETED');
    });

    // Case for destination site's replica object status.
    it('should set the global replication status to REPLICA if any backend ' +
    'status is REPLICA', () => {
        const objectMD = {
            replicationInfo: {
                status: '',
                backends: [
                    {
                        site: 'a',
                        status: 'REPLICA',
                    },
                    {
                        site: 'b',
                        status: 'COMPLETED',
                    },
                    {
                        site: 'c',
                        status: 'FAILED',
                    },
                ],
            },
        };
        const headers = collectResponseHeaders(objectMD);
        assert.strictEqual(headers['x-amz-replication-status'], 'REPLICA');
        assert.strictEqual(headers['x-amz-a-replication-status'], 'REPLICA');
        assert.strictEqual(headers['x-amz-b-replication-status'], 'COMPLETED');
        assert.strictEqual(headers['x-amz-c-replication-status'], 'FAILED');
    });

    [
        { md: { replicationInfo: null }, test: 'when config is not set' },
        { md: {}, test: 'for older objects' },
    ].forEach(item => {
        it(`should skip replication header ${item.test}`, () => {
            const headers = collectResponseHeaders(item.md);
            assert.deepStrictEqual(headers['x-amz-replication-status'],
                undefined);
        });
    });
});
