<template>
  <div>
    <nav class="navbar navbar-dark sticky-top bg-dark flex-md-nowrap p-0">
      <a class="navbar-brand col-sm-3 col-md-2 mr-0" href="#">Cortex</a>
      <ul class="navbar-nav px-3">
        <li class="nav-item text-nowrap">
          <a class="nav-link" href="#">Sign out</a>
        </li>
      </ul>
    </nav>

    <div class="container-fluid">
      <div class="row">
        <nav class="col-md-2 d-none d-md-block bg-light sidebar">
          <div class="sidebar-sticky">
            <ul class="nav flex-column">
              <li class="nav-item">
                <a class="nav-link" href="#" @click="currentTabComponent = 'Dashboard';">
                  Dashboard
                </a>
              </li>
              <li class="nav-item">
                <a class="nav-link" href="#" @click="currentTabComponent = 'Sources';">
                  Sources
                </a>
              </li>
              <li class="nav-item">
                <a class="nav-link" href="#" @click="currentTabComponent = 'Targets';">
                  Targets
                </a>
              </li>
              <li class="nav-item">
                <a class="nav-link" href="#" @click="currentTabComponent = 'Connections';">
                  Connections
                </a>
              </li>
            </ul>

          </div>
        </nav>

        <main role="main" class="col-md-9 ml-sm-auto col-lg-10 pt-3 px-4">
          <keep-alive>
            <component v-bind:is="currentTabComponent"></component>
          </keep-alive>
        </main>
      </div>
    </div>
  </div>
</template>

<script>
import Dashboard from './components/Dashboard.vue'
import Sources from './components/Sources.vue'
import Targets from './components/Targets.vue'
import Connections from './components/Connections.vue'

export default {
  name: 'app',
  components: {
    Dashboard,
    Sources,
    Targets,
    Connections
  },
  data: function () {
    return {
      currentTabComponent: Sources
    }
  }
}
</script>

<style>
#app {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
</style>
