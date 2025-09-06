# Based on: https://crane.dev/examples/quick-start-workspace.html

{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs =
    {
      nixpkgs,
      ...
    }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
    in
    {
      # Dev.
      devShells.${system}.default = pkgs.mkShell {
        buildInputs = with pkgs; [
          pkg-config
          rustPlatform.bindgenHook
          rust-jemalloc-sys-unprefixed
          udev
        ];
      };
    };
}
