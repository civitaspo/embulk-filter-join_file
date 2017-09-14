Embulk::JavaPlugin.register_filter(
  "join_file", "org.embulk.filter.join_file.JoinFileFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))

Embulk::JavaPlugin.register_output(
    "internal_forward", "org.embulk.filter.join_file.plugin.InternalForwardOutputPlugin",
    File.expand_path('../../../../classpath', __FILE__))
