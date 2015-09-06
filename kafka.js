var fs = require("fs");
var os = require("os");
var _ = require("lodash");
var request = require("request");
var dns = require("native-dns");
var async = require("async");
var child_process = require("child_process");

async.parallel({
    CLUSTER_LEADER: function(fn){
        var question = dns.Question({
          name: ["leaders", process.env.CS_CLUSTER_ID, "containership"].join("."),
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
    _.merge(kafka, process.env);

    _.defaults(kafka, {
        KAFKA_BROKER_ID: Math.floor((Math.random() * 256000000) + 1),
        KAFKA_PORT: 9092,
        KAFKA_ADVERTISED_PORT: 9092,
        ZOOKEEPER_APP_NAME: "zookeeper",
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
            config = config.replace(/{{ZOOKEEPER_CHROOT}}/g, kafka.ZOOKEEPER_CHROOT);
            return fn(null, config);
        },
        function(config, fn){
            var options = {
                url: ["http:/", [kafka.CLUSTER_LEADER, "8080"].join(":"), "v1", "applications", kafka.ZOOKEEPER_APP_NAME].join("/"),
                method: "GET",
                json: true,
                timeout: 5000
            }

            request(options, function(err, response){
                if(err)
                    return fn(err);
                else if(response && response.statusCode != 200)
                    return fn(new Error("Received non-200 status code from leader!"));
                else{
                    var zk_port = response.body.container_port;
                    async.map(_.pluck(response.body.containers, "host"), function(id, fn){
                        var options = {
                            url: ["http:/", [kafka.CLUSTER_LEADER, "8080"].join(":"), "v1", "hosts", id].join("/"),
                            method: "GET",
                            json: true,
                            timeout: 5000
                        }

                        request(options, function(err, response){
                            if(err)
                                return fn(null, "");
                            else
                                return fn(null, response.body.address.private);
                        });
                    }, function(err, zookeeper_ips){
                        zookeeper_ips = _.compact(zookeeper_ips);
                        zookeeper_ips = _.map(zookeeper_ips, function(zk_ip){
                            return [zk_ip, zk_port].join(":");
                        }).join(",");
                        config = config.replace(/{{ZOOKEEPER_IP}}:{{ZOOKEEPER_PORT}}/g, zookeeper_ips);
                        return fn(null, config);
                    });
                }
            });
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
