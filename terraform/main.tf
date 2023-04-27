provider "aws" {
    region = "us-east-1"
}

resource "aws_s3_bucket" "crypto_price_bucket_abro" {
  bucket = var.s3_bucket_identifier

  tags = {
    Name = "Crypto Price Bucket"
  }
}

resource "aws_redshift_cluster" "redshift_cluster" {
  cluster_identifier = var.cluster_identifier
  database_name = var.database_name
  master_username=var.master_username
  master_password=var.master_password
  node_type="dc2.large"
  skip_final_snapshot = "true"
  vpc_security_group_ids = [aws_security_group.redshift_security_group.id]
}

resource "aws_redshift_cluster_iam_roles" "example_iam_roles" {
  cluster_identifier = aws_redshift_cluster.redshift_cluster.cluster_identifier
  iam_role_arns = [aws_iam_role.s3_allow_iam.arn]
}

resource "aws_iam_role" "s3_allow_iam" {
  name="s3_allow_iam"

  assume_role_policy = jsonencode({
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
})

  inline_policy {
      name="redshift_inline_policy"
      policy = jsonencode({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "s3:*",
                "Resource": "*"
            }
        ]
      })
  }
}

resource "aws_default_vpc" "default" {
  tags= {
    name= "Default VPC"
  }
}

resource "aws_security_group" "redshift_security_group" {
  name="redshift_security_group"
  vpc_id = aws_default_vpc.default.id

  ingress {
    description = "Redshift_Port"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "allow all traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_vpc_endpoint" "redshift_endpoint_vpc" {
  vpc_id = aws_default_vpc.default.id
  service_name = "com.amazonaws.us-east-1.ec2"
  vpc_endpoint_type= "Interface"

  security_group_ids = [aws_security_group.redshift_security_group.id]
}