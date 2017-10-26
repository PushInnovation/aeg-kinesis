import * as chunk from 'chunk';
import * as _ from 'lodash';
import { EventEmitter } from 'events';
import * as  moment from 'moment-timezone';
import * as BBPromise from 'bluebird';
import { ClientConfiguration, PutRecordsInput } from 'aws-sdk/clients/kinesis';
import * as AWS from 'aws-sdk';
import { Segment } from 'aws-xray-sdk';

export interface IAWS {
	Kinesis: {
		new(config: AWS.Kinesis.Types.ClientConfiguration): AWS.Kinesis;
	};
}

export interface IKinesisEvent {
	type: string;
	timestamp: string;
	published: string;
	data: any;
	'for'?: string;
}

export default class Kinesis extends EventEmitter {

	private _kinesis: AWS.Kinesis;

	private _segment: Segment | undefined;

	constructor (credentials: ClientConfiguration, options: { aws?: IAWS, segment?: Segment } = {}) {

		super();

		const context = options.aws ? options.aws : AWS;
		this._kinesis = new context.Kinesis(credentials);
		this._segment = options.segment;

	}

	/**
	 * Write a batch of records to a stream with an event type and timestamp
	 * @param {string} stream - the stream to write to
	 * @param {string} type - the type of the event sent
	 * @param {Object} partition - useRecordProperty: use a record property,
	 *                             value: the shard key value or the record property name to use
	 * @param {Object[]} records - a single or an array of objects
	 * @param {moment} timestamp - event time
	 * @param {{[published]: moment | string, [audience]: string}} [options]
	 */
	public async write (
		stream: string,
		type: string,
		partition: { useRecordProperty?: boolean, value: string },
		records: any[] | any,
		timestamp: moment.Moment,
		options: { published?: moment.Moment | string, audience?: string } = {}) {

		const self = this;

		if (!records) {

			return;

		}

		// single record written to array
		if (!_.isArray(records)) {

			records = [records];

		}

		if (!records.length) {

			return;

		}

		self.emit('info', {
			message: 'written to stream',
			data: {stream, type, partition, count: records.length}
		});

		let published;
		let publishedIsRecordProperty = false;

		if (options.published) {

			if (moment.isMoment(options.published)) {

				published = options.published.tz('UTC').format('YYYY-MM-DD HH:mm:ss');

			} else {

				publishedIsRecordProperty = true;
				published = options.published;

			}

		} else {

			published = moment.tz('UTC').format('YYYY-MM-DD HH:mm:ss');

		}

		const kinesisRecords = _.map(records, (record) => {

			const event: IKinesisEvent = {
				type,
				timestamp: timestamp.tz('UTC').format('YYYY-MM-DD HH:mm:ss'),
				published: publishedIsRecordProperty ? record[published] : published,
				data: record
			};

			if (options.audience) {

				event.for = options.audience;

			}

			return event;

		});

		const batches: IKinesisEvent[][] = chunk(kinesisRecords, 500);

		return BBPromise.each(batches, (batch) => {

			return _writeBatchToStream(batch);

		});

		/**
		 * Write a batch of records
		 */
		async function _writeBatchToStream (batch: IKinesisEvent[]) {

			const data = _.map(batch, (record) => {

				const partitionKey = partition.useRecordProperty ? record.data[partition.value] : partition.value;
				return {Data: JSON.stringify(record), PartitionKey: String(partitionKey)};

			});

			const recordParams: PutRecordsInput & { Segment?: Segment } = {
				Records: data,
				StreamName: stream
			};

			if (self._segment) {

				recordParams.Segment = self._segment;

			}

			const putRecords: any = BBPromise.promisify(self._kinesis.putRecords, {context: self._kinesis});

			return putRecords(recordParams);

		}

	}

}
