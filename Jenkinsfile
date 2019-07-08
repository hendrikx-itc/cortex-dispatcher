pipeline {
    agent none
    stages {
        stage ('build') {
            agent {
                /*
                Mount the passwd file of the host into the container so that
                commands like scp can map the user Id to a name etc.

                Mount the workspace directory in the container to home
                directory mentioned in the passwd file, so that we have
                permissions to write the known_hosts file.
                */
                dockerfile {
                    filename 'packaging/dispatcher.dockerfile'
                    args '-v /etc/passwd:/etc/passwd -v ${WORKSPACE}:/home/jenkins-node'
                }
            }
            steps {
                withCredentials([string(credentialsId: 'controller-host-key', variable: 'HOST_KEY')]) {
                    sh "mkdir -p ~/.ssh && echo '${HOST_KEY}' > ~/.ssh/known_hosts"
                }

                dir('dispatcher') {
                    sh "CARGO_HOME=${WORKSPACE} cargo deb"
                }
                
                sshagent (credentials: ['615de4ee-505f-40d9-8994-1c53d4796725']) {
                    publishPackages 'target/debian', 'common/stable', 'xenial'
                }

                dir('sftp-scanner') {
                    sh "CARGO_HOME=${WORKSPACE} cargo deb"
                }
                
                sshagent (credentials: ['615de4ee-505f-40d9-8994-1c53d4796725']) {
                    publishPackages 'target/debian', 'common/stable', 'xenial'
                }
            }
        }
    }
}

