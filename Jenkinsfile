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
                
                withCredentials([usernamePassword(credentialsId: 'jenkins-nexus', passwordVariable: 'pass', usernameVariable: 'user')]) {
                    sh "curl -u '${user}:${pass}' -X POST -H 'Content-Type: multipart/form-data' --data-binary '@target/debian/dispatcher-0.1.2.deb' https://nexus.hendrikx-itc.nl/repository/hitc/"
                }

                dir('sftp-scanner') {
                    sh "CARGO_HOME=${WORKSPACE} cargo deb"
                }
                
                withCredentials([usernamePassword(credentialsId: 'jenkins-nexus', passwordVariable: 'pass', usernameVariable: 'user')]) {
                    sh "curl -u '${user}:${pass}' -X POST -H 'Content-Type: multipart/form-data' --data-binary '@target/debian/dispatcher-0.1.2.deb' https://nexus.hendrikx-itc.nl/repository/hitc/"
                }
            }
        }
    }
}

