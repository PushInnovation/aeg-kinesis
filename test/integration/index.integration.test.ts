import Kinesis from '../../src/index';
import * as moment from 'moment-timezone';
import * as xray from 'aws-xray-sdk';

xray.enableManualMode();

const aws = xray.captureAWS(require('aws-sdk'));

describe('index', async () => {

	describe('write', async () => {

		it('should return without error', async () => {

			const segment = new xray.Segment('test kinesis');

			const kinesis = new Kinesis({
				region: 'us-west-2',
				accessKeyId: 'X',
				secretAccessKey: 'X'
			}, {aws, segment});

			await kinesis.write('camp2-ci', 'test', {
				useRecordProperty: true,
				value: 'campId'
			}, [{campId: 1000}], moment.tz());

			segment.close();

		});

	});

});
