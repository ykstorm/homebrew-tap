cask "sbx" do
  version "0.33.0"
  sha256 "72b6347eca940cd8998084ed1f409d28c7d742064efb0d7f8baf07395a2a6eb7"

  url "https://github.com/docker/sbx-releases/releases/download/v#{version}/DockerSandboxes-darwin.tar.gz"
  name "Docker Sandboxes"
  desc "Build, run, and govern agents across the software development lifecycle"
  homepage "https://github.com/docker/sbx-releases"

  livecheck do
    url :url
    strategy :github_latest
  end

  conflicts_with cask: "docker/tap/sbx@nightly"
  depends_on arch:  :arm64,
             macos: :sonoma

  binary "bin/sbx", target: "sbx"
  bash_completion "completions/bash/sbx"
  fish_completion "completions/fish/sbx.fish"
  zsh_completion "completions/zsh/_sbx"

  uninstall_preflight do
    sbx_binary = "#{caskroom_path}/#{version}/bin/sbx"
    next unless File.exist?(sbx_binary)

    system_command sbx_binary,
                   args:         ["daemon", "stop"],
                   print_stderr: false
  end
end
