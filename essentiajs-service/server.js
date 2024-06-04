const Lame = require("node-lame").Lame;
const eureka = require("eureka-js-client").Eureka;
const express = require("express");
const multer = require("multer");

const { Essentia, EssentiaWASM, EssentiaModel } = require("essentia.js");
const essentia = new Essentia(EssentiaWASM);

const { Kafka } = require("kafkajs");
const kafka = new Kafka({
	clientId: "essentiajs-service",
	brokers: ["localhost:19092"],
});
const producer = kafka.producer();
producer.connect().then(() => {
	console.log("Connected to Kafka");
});

const app = express();
const port = 9097;
// Multer configuration
const storage = multer.diskStorage({
	destination: (req, file, cb) => {
		cb(null, "upload/");
	},
	filename: (req, file, cb) => {
		cb(null, file.originalname);
	},
});
const upload = multer({ storage: storage });

const client = new eureka({
	instance: {
		app: "ESSENTIAJS-SERVICE",
		instanceId: "ESSENTIAJS-SERVICE",
		hostName: "localhost",
		ipAddr: "127.0.0.1",
		vipAddress: "ESSENTIAJS-SERVICE",
		statusPageUrl: "http://localhost:9097/info",
		healthCheckUrl: "http://localhost:9097/info",
		port: {
			$: port,
			"@enabled": "true",
		},
		dataCenterInfo: {
			name: "MyOwn",
			"@class": "com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo",
		},
	},
	eureka: {
		host: "user:password@localhost",
		port: 8761,
		servicePath: "/eureka/apps/",
	},
});
/**
 * Analysis Funtion
 */
function analyze(rawdata) {
	var result = {};
	const buffer = Buffer.concat(rawdata);
	console.log("Analyzing audio");
	const encoder = new Lame({
		output: "buffer",
		raw: true,
	}).setBuffer(buffer);
	encoder
		.encode()
		.then(() => {
			const audio = encoder.getBuffer();
			const audioVector = essentia.arrayToVector(audio);
			let beats = essentia.RhythmExtractor(audioVector);
			let bpm = essentia.RhythmExtractor(audioVector).bpm;
			let danceability = essentia.Danceability(audioVector).danceability;
			result = {
				beats: beats ? beats.bpm : "120",
				danceability: danceability,
			};

			producer
				.send({
					topic: "audio_analysis",
					messages: [{ value: JSON.stringify(result) }],
				})
				.then(() => {
					console.log("Message sent to Kafka");
				});
			console.log(result);
			return result;
		})
		.catch((error) => {
			console.log("error: " + error);
			result = null;
		})
		.finally(() => {
			console.log("Audio analysis completed");
		});
}

/*
 * Express endpoints
 * */
app.get("/info", (req, res) => {
	res.send("Essentia.js service is running");
});
app.post("/audio/audio_analysis", (req, res) => {
	let audioData = [];
	req.on("data", (chunk) => {
		audioData.push(chunk);
	});
	req.on("end", () => {
		analyze(audioData);
		res.status(200).send("Audio analysis completed");
	});
});
app.listen(port, () => {
	console.log(`Essentia.js service is running on http://localhost:${port}`);
});
client.start();
