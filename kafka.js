var fs = require("fs");
var os = require("os");
var _ = require("lodash");
var request = require("request");
var dns = require("native-dns");
var async = require("async");
var child_process = require("child_process");

async.parallel({
    ZOOKEEPER_HOST: function(fn){
        var question = dns.Question({
          name: process.env.ZOOKEEPER_HOST,
          type: "A"
        });

        var req = dns.Request({
            question: question,
            server: { address: "127.0.0.1", port: 53, type: "udp" },
            timeout: 2000
        });

        req.on("timeout", function(){
            return fn();
        });

        req.on("message", function (err, answer) {
            var addresses = [];
            answer.answer.forEach(function(a){
                addresses.push(a.address);
            });

            return fn(null, _.first(addresses));
        });

        req.send();
    },
    KAFKA_ADVERTISED_HOST_NAME: function(fn){
        var question = dns.Question({
          name: [os.hostname(), process.env.CS_CLUSTER_ID, "containership"].join("."),
          type: "A"
        });

        var req = dns.Request({
            question: question,
            server: { address: "127.0.0.1", port: 53, type: "udp" },
            timeout: 2000
        });

        req.on("timeout", function(){
            return fn(null, "127.0.0.1");
        });

        req.on("message", function (err, answer) {
            var addresses = [];
            answer.answer.forEach(function(a){
                addresses.push(a.address);
            });

            return fn(null, _.first(addresses));
        });

        req.send();
    }
}, function(err, kafka){
    _.defaults(kafka, process.env);

    _.defaults(kafka, {
        KAFKA_BROKER_ID: Math.floor((Math.random() * 256000000) + 1),
        KAFKA_PORT: 9092,
        KAFKA_ADVERTISED_PORT: 9092,
        ZOOKEEPER_HOST: "localhost",
        ZOOKEEPER_PORT: 2181,
        ZOOKEEPER_CHROOT: "/kafka"
    });

    var template_location = ["", "kafka", "config", "server.properties.template"].join("/");
    var config_location = ["", "kafka", "config", "server.properties"].join("/");

    async.waterfall([
        function(fn){
            fs.readFile(template_location, fn);
        },
        function(config, fn){
            config = config.toString();
            config = config.replace(/{{KAFKA_BROKER_ID}}/g, kafka.KAFKA_BROKER_ID);
            config = config.replace(/{{KAFKA_ADVERTISED_HOST_NAME}}/g, kafka.KAFKA_ADVERTISED_HOST_NAME);
            config = config.replace(/{{KAFKA_PORT}}/g, kafka.KAFKA_PORT);
            config = config.replace(/{{KAFKA_ADVERTISED_PORT}}/g, kafka.KAFKA_ADVERTISED_PORT);
            config = config.replace(/{{ZOOKEEPER_IP}}/g, kafka.ZOOKEEPER_HOST);
            config = config.replace(/{{ZOOKEEPER_PORT}}/g, kafka.ZOOKEEPER_PORT);
            config = config.replace(/{{ZOOKEEPER_CHROOT}}/g, kafka.ZOOKEEPER_CHROOT);
            return fn(null, config);
        },
        function(config, fn){
            fs.writeFile(config_location, config, fn);
        }
    ], function(err){
        if(err){
            process.stderr.write(err.message);
            process.exit(1);
        }

        var proc = child_process.spawn(["", "kafka", "bin", "kafka-server-start.sh"].join("/"), [ config_location ]);

        proc.stdout.pipe(process.stdout);
        proc.stderr.pipe(process.stderr);

        proc.on("error", function(err){
            process.stderr.write(err.message);
            process.exit(1);
        });
    });

});
