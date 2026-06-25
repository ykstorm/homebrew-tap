# Example cask: identical to the upstream sbx cask except `url` points at a
# local Burrow sidecar (GET /artifact/{name}/{version}) instead of GitHub.
# The sidecar returns byte-identical bytes, so `sha256` is unchanged and
# `brew`'s integrity check still passes.
cask "sbx-burrow" do
  version "0.33.0"
  sha256 "72b6347eca940cd8998084ed1f409d28c7d742064efb0d7f8baf07395a2a6eb7"

  url "http://127.0.0.1:7777/artifact/sbx/#{version}"
  name "Docker Sandboxes (via Burrow)"
  desc "Build, run, and govern agents across the software development lifecycle"
  homepage "https://github.com/docker/sbx-releases"

  depends_on arch:  :arm64,
             macos: :sonoma

  binary "bin/sbx", target: "sbx"
end
