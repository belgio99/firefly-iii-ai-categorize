import express from "express";
import { getConfigVariable, debug } from "./util.js";
import FireflyService from "./FireflyService.js";
import OpenAiService from "./OpenAiService.js";
import { Server } from "socket.io";
import * as http from "http";
import Queue from "queue";
import JobList from "./JobList.js";

export default class App {
    #PORT;
    #ENABLE_UI;

    #firefly;
    #openAi;

    #server;
    #io;
    #express;

    #queue;
    #jobList;

    constructor() {
        this.#PORT = getConfigVariable("PORT", '3000');
        this.#ENABLE_UI = getConfigVariable("ENABLE_UI", 'false') === 'true';
        debug("App constructor: PORT =", this.#PORT, "ENABLE_UI =", this.#ENABLE_UI);
    }

    async run() {
        debug("App.run() starting");
        this.#firefly = new FireflyService();
        this.#openAi = new OpenAiService();

        this.#queue = new Queue({
            timeout: 30 * 1000,
            concurrency: 1,
            autostart: true
        });
        debug("Queue initialized");

        this.#queue.addEventListener('start', job => debug('Job started', job));
        this.#queue.addEventListener('success', event => debug('Job success', event.job));
        this.#queue.addEventListener('error', event => {
            debug('Job error event:', event);
            console.error('Job error', event.job, event.err, event);
        });
        this.#queue.addEventListener('timeout', event => debug('Job timeout', event.job));

        this.#express = express();
        this.#server = http.createServer(this.#express);
        this.#io = new Server(this.#server);
        debug("Express, server and socket.io initialized");

        this.#jobList = new JobList();
        this.#jobList.on('job created', data => {
            debug("Emitting event 'job created'", data);
            this.#io.emit('job created', data);
        });
        this.#jobList.on('job updated', data => {
            debug("Emitting event 'job updated'", data);
            this.#io.emit('job updated', data);
        });

        this.#express.use(express.json());
        debug("Middleware express.json() added");

        if (this.#ENABLE_UI) {
            this.#express.use('/', express.static('public'));
            debug("Static UI enabled");
        }

        this.#express.post('/webhook', this.#onWebhook.bind(this));

        this.#server.listen(this.#PORT, async () => {
            console.log(`Application running on port ${this.#PORT}`);
            debug("Server listening on port", this.#PORT);
        });

        this.#io.on('connection', socket => {
            debug('New socket.io connection established');
            console.log('connected');
            socket.emit('jobs', Array.from(this.#jobList.getJobs().values()));
        });
    }

    #onWebhook(req, res) {
        try {
            console.info("Webhook triggered");
            debug("Webhook request received:", req.body);
            debug("Transaction content", req.body?.content?.transactions[0]);
            this.#handleWebhook(req, res);
            res.send("Queued");
        } catch (e) {
            console.error(e);
            res.status(400).send(e.message);
        }
    }

    #handleWebhook(req, res) {
        // TODO: validate auth

        if (req.body?.trigger !== "STORE_TRANSACTION") {
            throw new WebhookException("trigger is not STORE_TRANSACTION. Request will not be processed");
        }

        if (req.body?.response !== "TRANSACTIONS") {
            throw new WebhookException("trigger is not TRANSACTION. Request will not be processed");
        }

        if (!req.body?.content?.id) {
            throw new WebhookException("Missing content.id");
        }

        if (req.body?.content?.transactions?.length === 0) {
            throw new WebhookException("No transactions are available in content.transactions");
        }

        if (req.body.content.transactions[0].type !== "withdrawal") {
            throw new WebhookException("content.transactions[0].type has to be 'withdrawal'. Transaction will be ignored.");
        }

        if (req.body.content.transactions[0].category_id) {
            throw new WebhookException("content.transactions[0].category_id is already set. Transaction will be ignored.");
        }

        if (!req.body.content.transactions[0].description) {
            throw new WebhookException("Missing content.transactions[0].description");
        }

        if (!req.body.content.transactions[0].destination_name) {
            throw new WebhookException("Missing content.transactions[0].destination_name");
        }

        const destinationName = req.body.content.transactions[0].destination_name;
        const description = req.body.content.transactions[0].description;

        const job = this.#jobList.createJob({
            destinationName,
            description
        });
        debug("Job created in webhook:", job);

        this.#queue.push(async () => {
            try {
                this.#jobList.setJobInProgress(job.id);
                debug(`Processing job id ${job.id}`);

                const categories = await this.#firefly.getCategories();
                debug("Categories fetched:", Array.from(categories.entries()));

                const { category, prompt, response } = await this.#openAi.classify(
                    Array.from(categories.keys()),
                    destinationName,
                    description
                );
                debug("Classification result:", { category, prompt, response });

                const newData = { ...job.data, category, prompt, response };
                this.#jobList.updateJobData(job.id, newData);
                debug("Job data updated with classification", newData);

                if (category) {
                    await this.#firefly.setCategory(req.body.content.id, req.body.content.transactions, categories.get(category));
                    debug("Category set in Firefly");
                }

                this.#jobList.setJobFinished(job.id);
                debug(`Job id ${job.id} finished`);
            } catch (e) {
                debug("Error processing job id", job.id, e);
                throw e;
            }
        });
    }
}

class WebhookException extends Error {
    constructor(message) {
        super(message);
    }
}
