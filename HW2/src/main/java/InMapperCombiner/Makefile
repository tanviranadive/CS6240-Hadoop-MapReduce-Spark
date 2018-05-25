######################### LOCAL CONFIGURATION ###################################
package.name=InMapperCombiner
class.name=InMapperCombinerDriver
jar.name=InMapperCombiner.jar
jar.path=jar/${jar.name}
local.input=input
local.output=output
local.output.aws=aws-output

######################### AWS CONFIGURATION ###################################
aws.cluster.name=InMapperCombiner
aws.bucket.name=hw2-InMapperCombiner
aws.jar.path=${jar.path}
aws.emr.release=emr-5.2.1
aws.region=us-east-1
aws.subnet.id=subnet-7bda2374
aws.inputbucket.name=hw2-NoCombiner
aws.input=input/1991.csv
aws.output=output
aws.log.dir=logs
aws.num.nodes=6
aws.instance.type=m4.large

######################### LOCAL RUN ###################################

# first step compiles all .java files in given package
jar: 	$(package.name)/*.java build.gradle Makefile
	gradle build
	gradle shadowJar
	mkdir jar
	cp build/libs/$(package.name)-all.jar $(jar.path)

run: 	jar clean-output
	hadoop jar $(jar.path) $(local.input) $(local.output)

clean-output: 
	rm -rf $(local.output)

clean: 	clean-output
	rm -rf build bin jar .gradle test.log

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

# Main EMR launch.
cloud: upload-jar-aws delete-output-aws
	aws emr create-cluster \
		--name ${aws.cluster.name} \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job.name}","s3://${aws.inputbucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${aws.jar.path}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
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


