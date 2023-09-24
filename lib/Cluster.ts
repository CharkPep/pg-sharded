import murmurhash from "imurmurhash";
import path from "path";
import sqlite3 from 'sqlite3'
import {Database, open} from 'sqlite'
import pgPromise from "pg-promise";
import pg from "pg-promise/typescript/pg-subset";
const pgp = pgPromise();
let master : Database;

export type node = {
    id: string,
    key : number,
    host : string,
    port : number,
    db : string,
    user : string,
    password : string,
    cluster_id : string,
    keys : number,
    vhosts : number
}

export class Cluster {
    connectionPool : Map<string, pgPromise.IDatabase<{}, pg.IClient>> = new Map();

    constructor(private readonly clusterId : string, private readonly keys : number, private vHosts : number, configuration_db : Database) {
        master = configuration_db;
    }

    async findNodesResponsibility(nodes : node[]) : Promise<Map<string, [number, number][]>> {
        const nodesResponsibility : Map<string,[number, number][]> = new Map();
        for (let i = 0;i < nodes.length;i++) {
            const prevNode = nodes[i - 1] || nodes[nodes.length - 1];
            const node = nodes[i].id.split(":")[0]
            nodesResponsibility.set(node, [...(nodesResponsibility.get(node) || []), [prevNode.key, nodes[i].key]]);
        }

        return nodesResponsibility;
    }

    async createDistributedTable(config : { tableName: string, partitionKey: string, columns: Record<string, string>}) {
        await master.run('BEGIN TRANSACTION');
        await master.run(`CREATE TABLE IF NOT EXISTS sharded_tables (id INTEGER PRIMARY KEY, 
                partition_key TEXT, 
                table_name TEXT, 
                cluster_id VARCHAR(255),
                schema JSONB)
                `);
        if (await master.get('SELECT * FROM sharded_tables WHERE table_name = ?', config.tableName)) {
            throw new Error(`Table already exists in cluster ${this.clusterId}`);
        }
        await master.run(`INSERT INTO sharded_tables (partition_key, table_name, cluster_id, schema) VALUES (?, ?, ?, ?)`,
            config.partitionKey, config.tableName, this.clusterId, JSON.stringify(config.columns));
        const nodes = await this.getClusterNodes();
        const keyConstrains = await this.findNodesResponsibility(nodes);

        const promises = nodes.filter((node) => node.id.split(":")[1] === '0')
            .map(async (node, index) => {
                await this.checkConnection(node);
                const con = this.connectionPool.get(node.id.split(":")[0]);

                await con!.none('BEGIN TRANSACTION');
                await con!.none(`CREATE TABLE IF NOT EXISTS ${config.tableName} (
                        key INTEGER CHECK (${keyConstrains.get(node.id.split(":")[0])!.map((constraint) => `(key >= ${constraint[0]} ${constraint[1] > constraint[0] ? 'AND' : 'OR'} key < ${constraint[1]})`).join(" OR ")}), 
                        cluster_id VARCHAR(255), 
                        ${Object.keys(config.columns).map(
                    (column) => `${column} ${config.columns[column]}`
                )})`);
            });

        return await Promise.all(promises).then(async () => {
            await master.run('COMMIT');
            const commits = nodes.map(async (node) => {
                const con = this.connectionPool.get(node.id.split(":")[0]);
                await con!.none('COMMIT');
            });
            return Promise.all(commits);
        }).catch(async (err) => {
            await master.run('ROLLBACK');
            const rollbacks = nodes.map(async (node) => {
                const con = this.connectionPool.get(node.id.split(":")[0]);
                await con!.none('ROLLBACK');
            });
            return Promise.all(rollbacks).then(() => {
                throw err;
            });
        });

    }

    async checkConnection(node : Pick<node, "id" | "host" | "port" | "db" | "user" | "password" >) {
        if (!this.connectionPool.has(node.id.split(":")[0])) {
            this.connectionPool.set(`${node.id.split(":")[0]}`, pgp({
                host: node.host,
                port: node.port,
                database: node.db,
                user: node.user,
                password: node.password,
                max : 30
            }));
            console.log(`Connection to ${node.id.split(":")[0]} created`)
        }
    }

    async insert(tableName : string, values : Record<string, any>) {
        const shardingKey = await master.get(`SELECT * FROM sharded_tables WHERE table_name = "${tableName}" AND cluster_id = "${this.clusterId}"`) as {partition_key : string} | undefined | null;
        if (!shardingKey){
            throw new Error(`Table ${tableName} does not exist in cluster ${this.clusterId}`);
        }

        if(!values[shardingKey.partition_key]) {
            throw new Error(`Partition key ${shardingKey.partition_key} not found in values`);
        }

        const keyHash = murmurhash(values[shardingKey.partition_key].toString()).result() % this.keys;
        const node = await this.getKey(values[shardingKey.partition_key]);
        await this.checkConnection(node);
        const con = this.connectionPool.get(node.id.split(":")[0]);
        return await con!.none(`INSERT INTO ${tableName} 
                    (key, cluster_id, ${Object.keys(values).join(", ")}) VALUES ($1, $2, ${Object.keys(values).map(((_, index) => `$${index + 3}`))})`,
            [keyHash,this.clusterId, ...Object.values(values)]).catch((err) => {
            throw new Error(`Error inserting into ${tableName} in node ${node.id.split(":")[0]}: ${err}`)
        });

    }


    /// @description: select from a sharded table
    /// @param tableName: name of the table to select from
    /// @param key: select keys in format { $key : $value }, if partition key value is not provided, it will be selected from all shards
    async select(tableName : string, key? : Record<string, any>, limit = 2000, offset? : number) {
        const shardingKey = await master.get(`SELECT * FROM sharded_tables WHERE table_name = "${tableName}" AND cluster_id = "${this.clusterId}"`) as {partition_key : string} | undefined | null;
        if (!shardingKey){
            throw new Error(`Table ${tableName} does not exist in cluster ${this.clusterId}`);
        }

        if (key && key[shardingKey.partition_key]){
            const node = await this.getKey(key[shardingKey.partition_key]);
            await this.checkConnection(node);
            const con = this.connectionPool.get(node.id.split(":")[0])!;
            return await con.any(`SELECT * FROM ${tableName} WHERE ${Object.keys(key).map(((key, index) => `${key}=$${index + 1}`))} AND cluster_id = '${this.clusterId}'`, [...Object.values(key)]);

        }

    }

    async getKey(key : string) : Promise<node> {
        const nodes = await this.getClusterNodes();
        const hashFunction = murmurhash(key.toString());
        const hash = hashFunction.result()%this.keys;
        const node = nodes.find((node) => node.key >= hash);
        if (!node) {
            return nodes[0];
        }

        return node;
    }

    async addNode(newNode : Pick<node, "id" | "host" | "port" | "db" | "user" | "password" >) {
        if(await master.get('SELECT * FROM nodes WHERE id = ? OR host = ? AND port = ?', newNode.id, newNode.host, newNode.port)) {
            throw new Error("Node already exists");
        }

        await this.checkConnection(newNode);
        const cn = this.connectionPool.get(newNode.id.split(":")[0])!;

        await master.run('BEGIN EXCLUSIVE TRANSACTION').then(async () => {
            await this.initNodeMetadata(newNode);
            await this.initNodeTables(newNode);
            await this.MoveDataToNewNode(newNode);
            await master.run('COMMIT');
        }).catch(async (err) => {
            await cn.none('ROLLBACK');
            await master.run('ROLLBACK');
            throw err;
        });

    }

    private async initNodeMetadata(host : Pick<node, "id" | "host" | "port" | "db" | "user" | "password">) {
        await this.checkConnection(host);
        //Insert batch with nodes metadata to master
        const valuesToInsert : node[] = []
        for (let i = 0;i < this.vHosts;i++) {
            const hashFunction = murmurhash();
            const key = hashFunction.hash(`${host.id}:${i}`).result() % this.keys;
            const newNode = {...host, key, id: host.id + ":" + i};
            valuesToInsert.push({ id : newNode.id, host : newNode.host, port : newNode.port, db : newNode.db, user : newNode.user, password : newNode.password, key, cluster_id : this.clusterId, keys : this.keys, vhosts : this.vHosts });
        }

        return await master.run(`INSERT INTO nodes(id, host, port, db, user, password, key, cluster_id, keys, vhosts) VALUES ${valuesToInsert.map(() => "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").join(", ")}`,
            valuesToInsert.flatMap((node) => [node.id, node.host, node.port, node.db, node.user, node.password, node.key, this.clusterId, this.keys, this.vHosts]));

    }

    private async initNodeTables(host : Pick<node, "id" | "host" | "port" | "db" | "user" | "password">) {
        await this.checkConnection(host);
        const cn = this.connectionPool.get(host.id.split(":")[0])!;
        const tables = (await master.all('SELECT * FROM sharded_tables WHERE cluster_id = ?', this.clusterId)).map((node) => ({
            ...node,
            schema : JSON.parse(node.schema)
        })) as {table_name : string, partition_key : string, schema : Record<string, string>}[];
        const nodes = await this.getClusterNodes();
        const keyConstrains = await this.findNodesResponsibility(nodes);
        return await Promise.all(tables.map(async (table) => cn.none(`CREATE TABLE IF NOT EXISTS ${table.table_name} (
                    key INTEGER CHECK(${keyConstrains.get(host.id.split(":")[0])!.map((constraint) => `(key >= ${constraint[0]} ${constraint[1] > constraint[0] ? 'AND' : 'OR'} key < ${constraint[1]})`).join(" OR ")}), 
                    cluster_id VARCHAR(255), 
                    ${Object.keys(table.schema).map(
                (column) => `${column} ${table.schema[column]}`
            )})`)
        )).catch(async (err) => {
            await cn.none('ROLLBACK');
            throw err;
        });
    }

    private async findSequentialNodesResponsibilityInterval(nodes : node[], node : node) : Promise<[node, node]> {
        const nodesResponsibilityInterval = await this.findNodesResponsibility(nodes)
        const nodeResponsibilityInterval = nodesResponsibilityInterval.get(node.id.split(":")[0])!.sort((
            (a,b) => a[0] - b[0]
        ));
        
        // console.log(nodeResponsibilityInterval)
        let nodeIntervalIndex = nodeResponsibilityInterval.findIndex((interval) => interval[1] === node.key);
        const initialNodeIntervalIndex = nodeIntervalIndex;
        while(nodeResponsibilityInterval[nodeIntervalIndex][0] === nodeResponsibilityInterval[nodeIntervalIndex - 1 >= 0 ? nodeIntervalIndex - 1 : nodeResponsibilityInterval.length - 1][1]) {
            nodeIntervalIndex = nodeIntervalIndex - 1 >= 0 ? nodeIntervalIndex - 1 : nodeResponsibilityInterval.length - 1;
            if(initialNodeIntervalIndex === nodeIntervalIndex){
                throw new Error("Node responsibility interval is not valid");
            }
        }
        // console.log(nodeResponsibilityInterval[nodeIntervalIndex][0], node.key)
        return [nodes.find((node) => node.key === nodeResponsibilityInterval[nodeIntervalIndex][0])!, node];
    }
    
    private async MoveDataToNewNode(host : Pick<node, "id" | "host" | "port" | "db" | "user" | "password">, range : number = 1000) {
        const nodes = await this.getClusterNodes();
        const newNodes = nodes.filter((node) => node.id.split(":")[0] === host.id)
        const tables = (await master.all('SELECT * FROM sharded_tables WHERE cluster_id = ?', this.clusterId)).map((node) => ({
            ...node,
            schema : JSON.parse(node.schema)
        })) as {table_name : string, partition_key : string, schema : Record<string, string>}[];
        await Promise.all(newNodes.map(async (node) => {
            const index = nodes.findIndex((n) => n.id === node.id);
            const nextNode = nodes[index + 1] || nodes[0];
            let prevNode = nodes[index - 1] || nodes[nodes.length - 1];
            //Node connection
            //Case when two or more nodes are sequentially placed
            if (nextNode.id.split(":")[0] === node.id.split(":")[0]){
                console.log('Warning: Same node sequence detected')
                return null;
            }
            
            const nodesResponsibilityInterval = await this.findNodesResponsibility(nodes);
            const vNodesInterval = await this.findSequentialNodesResponsibilityInterval(nodes, node)    
            const startKey = vNodesInterval[0].key;
            const endKey = vNodesInterval[1].key;
            
            await Promise.all([this.checkConnection(node), this.checkConnection(nextNode)]);
            const nextNodeCn = this.connectionPool.get(nextNode.id.split(":")[0])!;
            const cn = this.connectionPool.get(node.id.split(":")[0])!;
            return await Promise.all(tables.map(async (table) => {
                await nextNodeCn.none('BEGIN TRANSACTION');
                await nextNodeCn.none(`LOCK TABLE ${table.table_name} IN ACCESS EXCLUSIVE MODE`);
                let RowsToMove = await nextNodeCn.any(`SELECT * FROM ${table.table_name} WHERE key > $1 ${startKey > endKey ? 'OR' : 'AND' } key <= $2 LIMIT ${range}`,
                    [startKey, endKey]);
                // console.log(node.id, RowsToMove, startKey, endKey, `SELECT * FROM ${table.table_name} WHERE key > $1 ${startKey > endKey ? 'OR' : 'AND' } key <= $2 LIMIT ${range}`, [startKey, endKey]);
                while(RowsToMove.length > 0) {
                    await cn.none(pgp.helpers.insert(RowsToMove.map((row) => ({key : row.key, cluster_id : this.clusterId, ...row})),
                        new pgp.helpers.ColumnSet(['key', 'cluster_id', ...Object.keys(table.schema)], {table : table.table_name})));
                    await nextNodeCn.none(`DELETE FROM ${table.table_name} WHERE key > $1 ${startKey > endKey ? 'OR' : 'AND' } key <= $2`, [startKey, endKey]);
                    RowsToMove = await nextNodeCn.any(`SELECT * FROM ${table.table_name} WHERE key > $1 ${startKey > endKey ? 'OR' : 'AND' } key <= $2`,
                        [startKey, endKey]);
                }
                await this.checkConnection(prevNode);
                // console.log(prevNode.id);
                const prevNodeCn = this.connectionPool.get(prevNode.id.split(":")[0])!;
                await prevNodeCn.task(async (tx) => {
                    await tx.none(`ALTER TABLE ${table.table_name} DROP CONSTRAINT ${table.table_name}_key_check`);
                    await tx.none(`ALTER TABLE ${table.table_name} ADD CONSTRAINT ${table.table_name}_key_check CHECK
                        (${nodesResponsibilityInterval.get(prevNode.id.split(":")[0])!.map((constraint) => `(key >= ${constraint[0]} ${constraint[1] > constraint[0] ? 'AND' : 'OR'} key < ${constraint[1]})`).join(" OR ")})`);
                }).catch(async (err) => {
                    console.log(prevNode.id);
                    await prevNodeCn.none('ROLLBACK');
                    throw err;
                });
                await nextNodeCn.none('COMMIT');
            })).catch(async (err) => {
                console.log('Error moving data');
                await nextNodeCn.none('ROLLBACK');
                await cn.none('ROLLBACK');
                throw err;
            });

        })).catch(async (err) => {
            throw err;
        });

    }

    private async firstPrevNode(node : Pick<node, "key">, nodes? : node[]) {
        nodes = nodes || await this.getClusterNodes();
        return nodes.find((n) => n.key < node.key) || nodes[nodes.length - 1];
    }

    async getClusterNodes() {
        let nodes : node[];
        nodes = await master.all('SELECT * FROM nodes WHERE cluster_id = ?', this.clusterId).catch((err) => {
            console.log(`Error getting cluster nodes: ${err}`);
            throw err;
        }) as node[];
        nodes.sort((a, b) => a.key - b.key);
        return nodes;
    }

}