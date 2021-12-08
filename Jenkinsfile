def getEnvFromBranch(branch) {
  if (branch == 'master') {
    return 'prod'
  } else {
    return 'dev'
 }
}

pipeline {
    options {
        buildDiscarder(logRotator(numToKeepStr: '40'))
    }
    agent any
    environment {
        VERSION = 'latest'
        REGION = 'eu-west-1'
        ENV = getEnvFromBranch(env.BRANCH_NAME)
    }
    triggers {
        pollSCM('* * * * 1-5')
    }
    stages {
        stage('Build preparations') {
            steps {
                script {
                    if (ENV == "dev") {
                        ACCOUNT_ID = "338427658904"
                        ECRURL = "https://${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
                        ECRCRED = "ecr:${REGION}:datalake-infra-aws-${ENV}-account"
                    } else {
                        ACCOUNT_ID = "806624607236"
                        ECRURL = "https://${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
                        ECRCRED = "ecr:${REGION}:datalake-infra-aws-${ENV}-account"
                    }
                }
            }
        }
        stage('Create ECR Repository if not exist') {
            steps {
                withCredentials([[$class: 'AmazonWebServicesCredentialsBinding',
                                  credentialsId: "datalake-infra-aws-$ENV-account"]]) {
                    ansiColor('xterm') {
                        script {
                            sh """
                                if [[ \$(aws ecr describe-repositories | jq ".repositories | map(select(.repositoryName == \\"datalake/infra/c3\\")) | length") -eq 0 ]]
                                then
                                    aws ecr create-repository --repository-name "datalake/infra/c3"
                                fi
                            """
                        }
                    }
                }
            }
        }
        stage('Docker build') {
            steps {
                script  {
                    withCredentials([
                        [$class: 'AmazonWebServicesCredentialsBinding', credentialsId: "datalake-infra-aws-$ENV-account"]
                    ]) {
                        ansiColor('xterm') {
                            docker.build("${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/datalake/datalake/infra/c3:${VERSION}", "-f Dockerfile .")
                        }
                    }
                }
            }
        }
        stage('Docker push') {
            steps {
                script {
                    // Push the Docker image to ECR
                    docker.withRegistry(ECRURL, ECRCRED) {
                        ansiColor('xterm') {
                            docker.image("${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/datalake/${function}:${VERSION}").push()
                        }
                    }//end docker.withRegistry
                }//end script
            }//end steps
        }//end stage('Docker push')
    }
}