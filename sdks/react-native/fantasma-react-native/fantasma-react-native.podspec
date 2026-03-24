Pod::Spec.new do |s|
  s.name         = "fantasma-react-native"
  s.version      = "0.1.0"
  s.summary      = "Privacy-first React Native bridge for Fantasma."
  s.homepage     = "https://usefantasma.com"
  s.license      = { :type => "MIT" }
  s.author       = { "Fantasma" => "hello@usefantasma.com" }
  s.platforms    = { :ios => "17.0" }
  s.source       = { :git => "https://github.com/RuiAAPeres/fantasma.git", :tag => s.version.to_s }
  s.source_files = "ios/**/*.{swift,m}"
  s.swift_version = "5.10"
  s.dependency "React-Core"
end
