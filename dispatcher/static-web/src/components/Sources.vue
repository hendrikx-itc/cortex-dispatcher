<template>
  <div class="hello">
    <h1>{{ msg }}</h1>
    <p>
    API URL: {{ url }}
    </p>
    <h3>SFTP Sources</h3>
    <b-list-group>
      <b-list-group-item v-for="sftp_source in sftp_sources" v-bind:key="sftp_source.name" href="#">{{ sftp_source.name }}</b-list-group-item>
    </b-list-group>
  </div>
</template>

<script>
const axios = require('axios');

export default {
  name: 'Sources',
  props: {
    msg: String
  },
  data () {
    return {
      sftp_sources: null,
      url: process.env.API_BASE_URL
    }
  },
  mounted () {
    axios
      .get('http://localhost:56008/api/sftp-sources')
      .then(response => (this.sftp_sources = response.data))
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
h3 {
  margin: 40px 0 0;
}
a {
  color: #42b983;
  text-align: left;
}

</style>
