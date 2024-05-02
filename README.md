# Link to the report:
https://drive.google.com/file/d/1o5rMBz5d157CUZFVf5farpysJLxvs9Uq/view?usp=share_link

# How to Run the Program & the Test
## Starting the Worker Node

To start the worker node on your local machine, you can use the following command. This command sets the `PORT` environment variable to `7001` `7002` ... as defined in coordinatorNode.js, which specifies the port number on which the worker node will listen, and then starts the worker node script located at `./test/workerNode.js`.

```bash
PORT=7001 node ./test/workerNode.js
```
## Starting the Coordinator Node

```bash
PORT=7001 node ./test/coordinatorNode.js
```

## Run the workflow in coordinator.test.js
```bash
npx jest ./test/coordinator.test.js
```

