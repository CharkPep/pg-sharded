import murmurhash from "imurmurhash";
import {Cluster, node} from "./Cluster";
import pgPromise from "pg-promise";
import {Database, open} from "sqlite";
import sqlite3 from "sqlite3";
const pgp = pgPromise();
let master : Database;

export async function initMaster() {
    master = await open({
        filename: './master.db',
        driver: sqlite3.Database
    })

    await master.run("CREATE TABLE IF NOT EXISTS nodes (\
        id TEXT PRIMARY KEY, \
        host TEXT, port TEXT, \
        db TEXT, \
        user TEXT, \
        password TEXT, \
        key INTEGER, \
        cluster_id VARCHAR(255), \
        keys INTEGER, \
        vhosts INTEGER\
        )");

    await Promise.all([
        master.run('CREATE INDEX IF NOT EXISTS key_nodes ON nodes (key)'),
        master.run('CREATE INDEX IF NOT EXISTS id_nodes ON nodes (id)'),
    ]);

}

export class ClusterFactory {
    keys : number
    vHosts : number
    clusterId : string

    private async addNode(node : Pick<node, "id" | "host" | "port" | "db" | "user" | "password" >) : Promise<void> {
        const hashFunction = murmurhash();

        const pq = pgp({
            host: node.host,
            port: node.port,
            database: node.db,
            user: node.user,
            password: node.password,
            max : 1
        })
        const con = await pq.connect().catch(async (err) => {
            await master.run('ROLLBACK');
            throw new Error(node.id + " " + err);
        });

        con.done();


        for (let i = 0;i < this.vHosts;i++) {
            const key = hashFunction.hash(`${node.id}:{i}`).result() % this.keys;
            const newNode = {...node, key, id: node.id + ":" + i};
            try{
                await master.run('INSERT INTO nodes (id, host, port, db, user, password, key, cluster_id, keys, vhosts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    newNode.id, newNode.host, newNode.port, newNode.db, newNode.user, newNode.password, key, this.clusterId, this.keys, this.vHosts)
            }
            catch (err) {
                await master.run('ROLLBACK')
                throw err;
            }
        }

    }

    async getAllNodes() {
        return await master.all('SELECT * FROM nodes') as node[];
    }

    async open(clusterId : string){
        await initMaster();
        const nodes = await this.getAllNodes();
        if (nodes.length === 0) {
            throw new Error("nodes must be non-empty")
        }

        if (!nodes.find((node) => node.cluster_id === clusterId)) {
            throw new Error("clusterId not found")
        }

        return new Cluster(clusterId, nodes[0].keys, nodes[0].vhosts, master);
    }



    async discoverDistributedTables(tableName : string) {
        const clusterId = await master.get('SELECT * FROM sharded_tables WHERE table_name = ?', tableName) as {cluster_id : string} | undefined | null;
        if (!clusterId) {
            throw new Error(`Table ${tableName} not found`);
        }

        let nodes :  node[];
        try {
            nodes = await master.all('SELECT * FROM nodes WHERE cluster_id = ?', this.clusterId) as node[];
        }
        catch (err) {
            throw err;
        }

        if (!this.keys) {
            this.keys = nodes[0].keys;
        }

        nodes.sort((a, b) => a.key - b.key);

        this.keys = nodes[0].keys;

        return new Cluster(clusterId.cluster_id, nodes[0].keys, nodes[0].vhosts, master);

    }

    async init(nodes : Pick<node, "id" | "host" | "port" | "db" | "user" | "password" >[], keys : number, vHosts : number) {
        await initMaster();
        const hashFunction = murmurhash("node");
        if (nodes.length === 0) {
            throw new Error("nodes must be non-empty")
        }

        this.clusterId =  hashFunction.hash(nodes.map((node) => node.id).join(":")).result().toString(16);
        this.keys = keys;
        this.vHosts = vHosts;
        await master.run('BEGIN TRANSACTION').catch((err) => {
            throw err;
        });
        for (let i = 0;i < nodes.length;i++){
            await this.addNode(nodes[i]);
        }
        await master.run('COMMIT').catch((err) => {
            throw err;
        });

        return new Cluster(this.clusterId, keys, vHosts, master);

    }
    
}