'use strict';

const _ = require('lodash');
const async = require('async');
const child_process = require('child_process');
const dns = require('native-dns');
const fs = require('fs');
const os = require('os');
const request = require('request');

async.parallel({
	ZOOKEEPER_HOST: (callback) => {
		const question = dns.Question({
			name: process.env.ZOOKEEPER_HOST,
			type: 'A'
		});

		const req = dns.Request({
			question: question,
			server: { address: '127.0.0.1', port: 53, type: 'udp' },
			timeout: 2000
		});

		req.on('timeout', () => {
			return callback();
		});

		req.on('message', (err, answer) => {
			const addresses = [];
			answer.answer.forEach((a) => {
				addresses.push(a.address);
			});

			return callback(null, _.first(addresses));
		});

		req.send();
	},
    KAFKA_ADVERTISED_HOST_NAME: (callback) => {
        const question = dns.Question({
          name: `${os.hostname()}.${process.env.CS_CLUSTER_ID}.containership`,
          type: 'A'
        });

        const req = dns.Request({
            question: question,
            server: {
                address: '127.0.0.1',
                port: 53,
                type: 'udp'
            },
            timeout: 2000
        });

        req.on('timeout', () => {
            return callback(null, '127.0.0.1');
        });

        req.on('message', (err, answer) => {
            const addresses = [];
            answer.answer.forEach((a) => {
                addresses.push(a.address);
            });

            return callback(null, _.first(addresses));
        });

        req.send();
    }
}, (err, kafka) => {
    _.defaults(kafka, process.env);

    _.defaults(kafka, {
        KAFKA_BROKER_ID: Math.floor((Math.random() * 256000000) + 1),
        KAFKA_PORT: 9092,
        KAFKA_ADVERTISED_PORT: 9092,
        KAFKA_DELETE_TOPIC_ENABLE: false,
        GROUP_MAX_SESSION_TIMEOUT_MS: 30000,
        ZOOKEEPER_CHROOT: '/kafka',
        ZOOKEEPER_HOST: 'localhost',
        ZOOKEEPER_PORT: 2181,
        ZOOKEEPER_CONNECTION_TIMEOUT_MS: 6000,
        ZOOKEEPER_SESSION_TIMEOUT_MS: 6000
    });

    const template_location = '/kafka/config/server.properties.template';
    const config_location = '/kafka/config/server.properties';

    async.waterfall([
        (callback) => {
            fs.readFile(template_location, callback);
        },
        (config, callback) => {
            config = config.toString();
            config = config.replace(/{{KAFKA_BROKER_ID}}/g, kafka.KAFKA_BROKER_ID);
            config = config.replace(/{{KAFKA_ADVERTISED_HOST_NAME}}/g, kafka.KAFKA_ADVERTISED_HOST_NAME);
            config = config.replace(/{{KAFKA_PORT}}/g, kafka.KAFKA_PORT);
            config = config.replace(/{{KAFKA_ADVERTISED_PORT}}/g, kafka.KAFKA_ADVERTISED_PORT);
            config = config.replace(/{{KAFKA_DELETE_TOPIC_ENABLE}}/g, kafka.KAFKA_DELETE_TOPIC_ENABLE);

            config = config.replace(/{{ZOOKEEPER_CONNECTION_STRING}}/g, kafka.ZOOKEEPER_CONNECTION_STRING || `${kafka.ZOOKEEPER_HOST}:${kafka.ZOOKEEPER_PORT}/${kafka.ZOOKEEPER_CHROOT}`);
            config = config.replace(/{{ZOOKEEPER_CHROOT}}/g, kafka.ZOOKEEPER_CHROOT);
            config = config.replace(/{{ZOOKEEPER_CONNECTION_TIMEOUT_MS}}/g, kafka.ZOOKEEPER_CONNECTION_TIMEOUT_MS);
            config = config.replace(/{{ZOOKEEPER_SESSION_TIMEOUT_MS}}/g, kafka.ZOOKEEPER_SESSION_TIMEOUT_MS);

            config = config.replace(/{{GROUP_MAX_SESSION_TIMEOUT_MS}}/g, kafka.GROUP_MAX_SESSION_TIMEOUT_MS);

            return callback(null, config);
        },
        (config, callback) => {
            fs.writeFile(config_location, config, callback);
        }
    ], (err) => {
        if(err) {
            console.error(err.message);
            process.exit(1);
        }

        const proc = child_process.spawn('/kafka/bin/kafka-server-start.sh', [ config_location ]);

        proc.stdout.pipe(process.stdout);
        proc.stderr.pipe(process.stderr);

        proc.on('error', (err) => {
            console.error(err.message);
            process.exit(1);
        });
    });

});
