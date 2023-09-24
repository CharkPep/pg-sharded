import { ClusterFactory } from "./lib/ClusterFactory";


async function main() {
    
    // const cluster = await new ClusterFactory().init([{ 
    //     id: "node1",
    //     host: "localhost",
    //     port: 5432,
    //     db: "test",
    //     user : "pep",
    //     password : "0001"
    // }], 65546, 8)


    
    const cluster = await new ClusterFactory().open("6b3fdbd");
    // console.log(await cluster.select('test2', { age : 82 }));
    await cluster.createDistributedTable({ 
        tableName : 'test3', 
        partitionKey : 'city', 
        columns : { id : 'INTEGER PRIMARY KEY', city : 'VARCHAR(255)', foundationDate : 'Date' } });
    // for(let i = 101;i <= 200;i++) {
    //     await cluster.insert('test2', {id : i,  name: "Vlad", age: i - 100});
    // }

    // await cluster.addNode({ id : 'node2', host : 'localhost', port : 5433, user : 'pep', password : '0001', db : 'test' })
    process.exit(0);
}

main();