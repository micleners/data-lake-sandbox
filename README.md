This repository implements the base image from [svajiraya/glue-dev-1.0](https://hub.docker.com/r/svajiraya/glue-dev-1.0). This base image sets up the environment to run glue ETL via a PySpark server.

To run this dev environment, [Docker must be installed](https://docs.docker.com/get-docker/) locally. Then, the docker image can be built with the command:
- Build docker image: `docker build . -t data-lake-scripts`
- To run all the tests, run the command: `docker run --rm -v $PWD:/work data-lake-scripts` or `docker-compose up`
- To run the docker image and enter container, run the command: `docker run --rm -it -v $PWD:/work data-lake-scripts bash`
  - Once in the container, you can run tests with: `bin/gluepytest /work/glue_scripts/`
  - You can run a specific test file by drilling down to the suite like this: `bin/gluepytest /work/glue_scripts/tests/test_amod_current_dataset.py`

Our development environment exists within this project. It consists of:
- `Dockerfile`: The setup for this docker project
- `docker-compose.yml`: Orchestration of docker image (currently optional)
- `glue_scripts`: the directory to compose ETL scripts
- `glue_scripts/tests`: the directory for unit and integration tests for the scripts
- JavaScript related files. These were used to generate test data in an early spike:
  - `package.json`
  - `package-lock.json`
  - `generate-random-customers.js`
  - `node_modules`
- A cloud formation file that is currently not being kept up with (will clean when we merge projects)