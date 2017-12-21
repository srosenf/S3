var Readable = require('readable-stream').Readable

class MongoReadStream extends Readable {
    constructor(c, options) {
	super({
	    objectMode: true,
	    highWaterMark: 0
	});

	if (options.limit === 0)
	    return;

	var query = { _id: {} };
	if (options.reverse) {
	    if (options.start) query._id.$lte = options.start;
	    if (options.end)   query._id.$gte = options.end;
	    if (options.gt)    query._id.$lt  = options.gt;
	    if (options.gte)   query._id.$lte = options.gte;
	    if (options.lt)    query._id.$gt  = options.lt;
	    if (options.lte)   query._id.$gte = options.lte;
	} else {
	    if (options.start) query._id.$gte = options.start;
	    if (options.end)   query._id.$lte = options.end;
	    if (options.gt)    query._id.$gt  = options.gt;
	    if (options.gte)   query._id.$gte = options.gte;
	    if (options.lt)    query._id.$lt  = options.lt;
	    if (options.lte)   query._id.$lte = options.lte;
	}

	if (!Object.keys(query._id).length)
	    delete query._id;
	
	this._cursor = c.find(query).sort({ _id: options.reverse ? -1 : 1 });
	if (options.limit && options.limit !== -1)
	    this._cursor = this._cursor.limit(options.limit);
	this._options = options;
	this._destroyed = false;
	this.on('end', this._cleanup.bind(this));
    }
    
    _read() {
	if (this._destroyed)
	    return;
	
	this._cursor.next((err, doc) => {
	    if (this._destroyed)
		return;
	    if (err)
		return this.emit('error', err);

	    var key = undefined;
	    var value = undefined;
	    
	    if (doc) {
		var key = doc._id;
		var value = JSON.stringify(doc.value);
	    }
	    
	    if (key === undefined && value === undefined) {
		this.push(null);
	    } else if (this._options.keys !== false &&
		       this._options.values === false) {
		this.push(key);
	    } else if (this._options.keys === false &&
		       this._options.values !== false) {
		this.push(value);
	    } else {
		this.push({ key: key, value: value });
	    }
	});
    }

    _cleanup() {
	if (this._destroyed)
	    return;
	this._destroyed = true;

	this._cursor.close((err) => {
	    if (err)
		return this.emit('error', err);
	    this.emit('close');
	});
    }
    
    destroy() {
	return _cleanup();
    }
};

module.exports = MongoReadStream;
