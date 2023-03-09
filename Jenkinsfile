pipeline {
    agent {
        node {
            label 'docker'
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
                stash name: 'deb', includes: 'target/debian/*.deb'
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
                stash name: 'deb', includes: 'target/debian/*.deb'
            }
        }
        stage('publish-debian-packages') {
            steps {
                unstash name: 'deb'
                script {
                    publishPackages 'target/debian', 'kpn/focal/stable', 'focal'
                    publishPackages 'target/debian', 'kpn/jammy/stable', 'jammy'
                }
            }
        }
    }
}

