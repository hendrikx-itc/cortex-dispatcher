pipeline {
    agent {
        dockerfile {
            filename 'packaging/dispatcher.dockerfile'
            args "-e CARGO_HOME=$WORKSPACE"
        }
    }
    stage ('build') {
        steps {
            dir 'dispatcher' {
                sh 'cargo deb'
            }
        }
    }
}
