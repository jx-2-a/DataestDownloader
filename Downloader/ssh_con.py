import paramiko

class KeyboardInteractiveSSH:
    """
    封装 Keyboard-Interactive SSH 登录，返回可复用的 SSHClient
    """

    def __init__(self, hostname, username, port=22):
        self.hostname = hostname
        self.username = username
        self.port = port
        self.transport = None
        self.ssh = None

    def _handler(self, title, instructions, prompt_list):
        """
        Keyboard-Interactive 回调
        """
        responses = []
        for prompt, echo in prompt_list:
            resp = input(prompt)  # 阻塞等待用户输入动态码
            responses.append(resp)
        return responses

    def connect(self):
        """
        建立 SSH 连接并返回可复用的 SSHClient 对象
        """
        # 创建 Transport
        self.transport = paramiko.Transport((self.hostname, self.port))
        self.transport.start_client()

        # Keyboard-Interactive 认证
        self.transport.auth_interactive(self.username, self._handler)

        # 创建 SSHClient 并复用 Transport
        self.ssh = paramiko.SSHClient()
        self.ssh._transport = self.transport

        return self.ssh

    def close(self):
        """
        关闭 SSH 连接
        """
        if self.ssh:
            self.ssh.close()
        if self.transport:
            self.transport.close()
