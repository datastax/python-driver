# Basic virtualbox configuration
Exec { path => "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" }

node basenode {
  package{["build-essential", "git-core", "vim"]:
    ensure => installed
  }
}

class xfstools {
    package{['lvm2', 'xfsprogs']:
        ensure => installed
    }
}
class java {
    package {['openjdk-7-jre-headless']:
        ensure => installed 
    }
}

class cassandra {
  include xfstools
  include java

  package {"wget":
    ensure => latest
  }

  file {"/etc/init/cassandra.conf":
    source => "puppet:///modules/cassandra/cassandra.upstart",
    owner  => root
  }
  
  exec {"download-cassandra":
    cwd => "/tmp",
    command => "wget http://download.nextag.com/apache/cassandra/1.2.19/apache-cassandra-1.2.19-bin.tar.gz",
    creates => "/tmp/apache-cassandra-1.2.19-bin.tar.gz",
    require => [Package["wget"], File["/etc/init/cassandra.conf"]]
  }

  exec {"install-cassandra":
    cwd => "/tmp",
    command => "tar -xzf apache-cassandra-1.2.19-bin.tar.gz; mv apache-cassandra-1.2.19 /usr/local/cassandra",
    require => Exec["download-cassandra"],
    creates => "/usr/local/cassandra/bin/cassandra"
  }

  service {"cassandra":
    ensure => running,
    require => Exec["install-cassandra"]
  }
}

node cassandraengine inherits basenode {
  include cassandra
  
  package {["python-pip", "python-dev", "python-nose"]:
    ensure => installed
  }

  exec {"install-requirements":
    cwd => "/vagrant",
    command => "pip install -r requirements-dev.txt",
    require => [Package["python-pip"], Package["python-dev"]]
  }
}
