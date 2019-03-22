node ('git') {
    stage ('checkout') {
        checkout scm
    }

    stage ('build') {
        // Report status of stage 'build' back to Gitlab
        gitlabCommitStatus(name: 'build') {
            def buildDir = 'pkg-build'

            // Clean the build directory before starting
            sh "rm -rf ${buildDir}"
            sh "mkdir -p ${buildDir}"

            // Credentials used should be stored in Jenkins under the name 'hitc-docker-registry'
            docker.withRegistry("https://${docker_registry}", 'hitc-docker-registry') {
                docker.image('ubuntu-1604-packaging').inside("-u root:root -v ${workspace}:/package/source -v ${workspace}/${buildDir}:/package/build") {
                    sh 'package'
                }
            }

            publishPackages buildDir.toString(), 'common/stable', 'xenial'

            archiveArtifacts(artifacts: "${buildDir}/*")
        }
    }
}
