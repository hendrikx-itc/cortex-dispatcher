pipeline {
    agent {
        dockerfile {
            filename 'packaging/dispatcher.dockerfile'
            args "-e CARGO_HOME=${env.WORKSPACE}"
        }
    }
    stages {
        stage ('build') {
            steps {
                dir('dispatcher') {
                    sh 'cargo deb'
                }

                dir('sftp-scanner') {
                    sh 'cargo deb'
                }
            }
        }
    }
}
