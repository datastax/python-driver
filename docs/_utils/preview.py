from livereload import Server, shell
server = Server()
server.watch('*.rst', shell('make dirhtml'))
server.watch('*.md', shell('make dirhtml'))
server.serve(host='localhost', root='_build/dirhtml')
