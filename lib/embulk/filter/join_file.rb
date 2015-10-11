Embulk::JavaPlugin.register_filter(
  "join_file", "org.embulk.filter.join_file.JoinFileFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
