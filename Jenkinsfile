pipeline {
    agent {
        node {
            label 'git'
        }
    }
    stages {
        stage ('build-dispatcher') {
            agent {
                dockerfile {
                    filename 'packaging/dispatcher.dockerfile'
                }
            }
            steps {
                dir('dispatcher') {
                    sh "CARGO_HOME=${WORKSPACE} cargo deb"
                }
            }
        }
        stage('publish-dispatcher') {
            agent {
                node {
                    label 'git'
                }
            }
            steps {
                script {
                    publishPackages 'target/debian', 'kpn/bionic/stable', 'bionic'
                }
            }
        }
        stage ('build-sftp-scanner') {
            agent {
                dockerfile {
                    filename 'packaging/dispatcher.dockerfile'
                }
            }
            steps {
                dir('sftp-scanner') {
                    sh "CARGO_HOME=${WORKSPACE} cargo deb"
                }
            }
        }
        stage('publish-sftp-scanner') {
            agent {
                node {
                    label 'git'
                }
            }
            steps {
                script {
                    publishPackages 'target/debian', 'kpn/bionic/stable', 'bionic'
                }
            }
        }
    }
}

