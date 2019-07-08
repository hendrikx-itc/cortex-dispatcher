pipeline {
    agent {
        dockerfile {
            filename 'packaging/dispatcher.dockerfile'
        }
    }
    stages {
        stage ('build') {
            steps {
                dir('dispatcher') {
                    sh "CARGO_HOME=${WORKSPACE} cargo deb"
                }
                
                sshagent (credentials: ['jenkins-node']) {
                    publishPackages 'target/debian', 'common/stable', 'xenial'
                }

                dir('sftp-scanner') {
                    sh "CARGO_HOME=${WORKSPACE} cargo deb"
                }
                
                sshagent (credentials: ['jenkins-node']) {
                    publishPackages 'target/debian', 'common/stable', 'xenial'
                }
            }
        }
    }
}

