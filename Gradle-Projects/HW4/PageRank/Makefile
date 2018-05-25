######################### LOCAL CONFIGURATION ###################################
package.name=""
class.name=PageRank
job.name = PageRank
jar.name=PageRank.jar
jar.path=${jar.name}
local.input=input
local.output=output/
local.output.aws=aws-output

######################### AWS CONFIGURATION ###################################
aws.cluster.name=PageRank
aws.bucket.name=hw4-PageRank
aws.jar.path=jar/${jar.path}
aws.emr.release=emr-5.2.1
aws.region=us-east-1
aws.subnet.id=subnet-7bda2374
aws.input=input
#aws.output=output/
aws.output=outputRun2/
#aws.log.dir=logs
aws.log.dir=logs2
aws.num.nodes=11
aws.instance.type=m4.large

######################### LOCAL RUN ###################################
jar:
	sbt package
	cp target/scala-*/pagerank_*.jar PageRank.jar

run: jar
	rm -rf output
	sbt "run input/wikipedia-simple-html.bz2 output"

clean:	rm -rf project output target *.jar output



######################### AWS RUN ###################################

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# delete S3 bucket
delete-bucket:
	aws s3 rb s3://${aws.bucket.name} --force

# Upload data to S3 input dir.
upload-input-aws: 
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

# Upload application to S3 bucket.
upload-jar-aws:
	aws s3 rm s3://${aws.bucket.name}/jar --recursive
	aws s3 cp ${jar.path} s3://${aws.bucket.name}/jar/




create-cluster: upload-jar-aws delete-output-aws
	aws emr create-cluster \
		--name ${aws.cluster.name} \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
		  --applications Name=Spark \
		  --steps '[{"Name":"PageRank", "Args":["--class", "${job.name}", "--master", "yarn", "--deploy-mode", "cluster", "s3://${aws.bucket.name}/${aws.jar.path}", "s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"],"Type":"Spark","Jar":"s3://${aws.bucket.name}/${aws.jar.path}","ActionOnFailure":"TERMINATE_CLUSTER"}]' \
		    --log-uri s3://${aws.bucket.name}/${aws.log.dir} \
			--service-role EMR_DefaultRole \
			--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
			--region ${aws.region} \
			--enable-debugging \
	--auto-terminate


# Main EMR launch.
cloud:		delete-output-aws
		aws emr create-cluster \
		--name ${aws.cluster.name} \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${jar.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--master","yarn","--deploy-mode","cluster","--class","${job.name}","s3://${aws.bucket.name}/${aws.jar.path}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"] \
		 --log-uri s3://${aws.bucket.name}/${aws.log.dir} \
			--service-role EMR_DefaultRole \
			--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
			--region ${aws.region} \
			--enable-debugging \
	--auto-terminate


# Download output from S3.
download-output-aws: 
	rm -rf ${local.output.aws} 
	mkdir ${local.output.aws}
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${local.output.aws}

