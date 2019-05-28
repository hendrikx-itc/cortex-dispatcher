<template>
  <div class="Targets">
    <h3>SFTP Targets</h3>
    <b-list-group>
      <b-list-group-item v-for="sftp_target in sftp_targets" v-bind:key="sftp_target.name" href="#">{{ sftp_target.name }}</b-list-group-item>
    </b-list-group>
  </div>
</template>

<script>
const axios = require('axios');

export default {
  name: 'Targets',
  props: {
    msg: String
  },
  data () {
    return {
      sftp_targets: null,
      url: process.env.API_BASE_URL
    }
  },
  mounted () {
    axios
      .get('http://localhost:56008/api/directory-targets')
      .then(response => (this.sftp_targets = response.data))
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
h3 {
  margin: 5px 0 40px;
}
</style>
