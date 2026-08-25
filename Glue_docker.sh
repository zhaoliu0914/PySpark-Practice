docker pull public.ecr.aws/glue/aws-glue-libs:5 

PROFILE_NAME="zhaoliu"

WORKSPACE_LOCATION=/home/zhaoliu/pyspark
SCRIPT_FILE_NAME=hello_world.py
mkdir -p ${WORKSPACE_LOCATION}/src
vim ${WORKSPACE_LOCATION}/src/${SCRIPT_FILE_NAME}


# Run Glue on docker
sudo docker run -it --rm \
    -v ~/.aws:/home/hadoop/.aws \
    -v /home/zhaoliu/PySpark-Practice:/home/hadoop/workspace/ \
    -e AWS_PROFILE="zhaoliu" \
    --name glue5_spark_submit \
    public.ecr.aws/glue/aws-glue-libs:5 \
    spark-submit /home/hadoop/workspace/src/hello_world.py


# Check for file exist or not, or other errors.
sudo docker run -it --rm \
    -v /home/zhaoliu/PySpark-Practice:/home/hadoop/workspace/ \
    public.ecr.aws/glue/aws-glue-libs:5 \
    -c "ls -la /home/hadoop/workspace/ /home/hadoop/workspace/data/"