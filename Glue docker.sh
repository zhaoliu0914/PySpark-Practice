docker pull public.ecr.aws/glue/aws-glue-libs:5 

PROFILE_NAME="zhaoliu"

WORKSPACE_LOCATION=/home/zhaoliu/pyspark
SCRIPT_FILE_NAME=hello_world.py
mkdir -p ${WORKSPACE_LOCATION}/src
vim ${WORKSPACE_LOCATION}/src/${SCRIPT_FILE_NAME}



sudo docker run -it --rm \
    -v ~/.aws:/home/hadoop/.aws \
    -v $WORKSPACE_LOCATION:/home/hadoop/workspace/ \
    -e AWS_PROFILE=$PROFILE_NAME \
    --name glue5_spark_submit \
    public.ecr.aws/glue/aws-glue-libs:5 \
    spark-submit /home/hadoop/workspace/src/$SCRIPT_FILE_NAME