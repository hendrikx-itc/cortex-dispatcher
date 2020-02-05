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

                script {
                    switch (GIT_BRANCH) {
                        case "origin/master":
                            publishPackages 'target/debian', 'kpn/bionic/stable', 'bionic'
                            break
                        case "origin/develop":
                            publishPackages 'target/debian', 'kpn/bionic/unstable', 'bionic'
                            break
                    }
                }
                
                dir('sftp-scanner') {
                    sh "CARGO_HOME=${WORKSPACE} cargo deb"
                }
                
                script {
                    switch (GIT_BRANCH) {
                        case "origin/master":
                            publishPackages 'target/debian', 'kpn/bionic/stable', 'bionic'
                            break
                        case "origin/develop":
                            publishPackages 'target/debian', 'kpn/bionic/unstable', 'bionic'
                            break
                    }
                }
            }
        }
    }
}

