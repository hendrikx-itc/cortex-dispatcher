<template>
  <div class="Connections">
    <h3>Connections</h3>
    <b-list-group>
      <b-list-group-item v-for="connection in connections" v-bind:key="connection.name" href="#">{{ connection.source }} -> {{ connection.target }}</b-list-group-item>
    </b-list-group>
  </div>
</template>

<script>
const axios = require('axios');

export default {
  name: 'Connections',
  props: {
    msg: String
  },
  data () {
    return {
      connections: null,
      url: process.env.API_BASE_URL
    }
  },
  mounted () {
    axios
      .get('http://localhost:56008/api/connections')
      .then(response => (this.connections = response.data))
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
h3 {
  margin: 5px 0 40px;
}
</style>
