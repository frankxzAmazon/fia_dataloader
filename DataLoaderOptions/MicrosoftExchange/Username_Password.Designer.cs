namespace DataLoaderOptions.MicrosoftExchange
{
    partial class Username_Password
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.userName = new System.Windows.Forms.TextBox();
            this.password = new System.Windows.Forms.TextBox();
            this.logIn = new System.Windows.Forms.Button();
            this.cancel = new System.Windows.Forms.Button();
            this.addressName = new System.Windows.Forms.Label();
            this.passwordName = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // userName
            // 
            this.userName.Location = new System.Drawing.Point(34, 62);
            this.userName.Name = "userName";
            this.userName.Size = new System.Drawing.Size(213, 20);
            this.userName.TabIndex = 0;
            // 
            // password
            // 
            this.password.Location = new System.Drawing.Point(34, 139);
            this.password.Name = "password";
            this.password.Size = new System.Drawing.Size(213, 20);
            this.password.TabIndex = 1;
            this.password.UseSystemPasswordChar = true;
            // 
            // logIn
            // 
            this.logIn.Location = new System.Drawing.Point(34, 184);
            this.logIn.Name = "logIn";
            this.logIn.Size = new System.Drawing.Size(74, 35);
            this.logIn.TabIndex = 2;
            this.logIn.Text = "Log In";
            this.logIn.UseVisualStyleBackColor = true;
            // 
            // cancel
            // 
            this.cancel.Location = new System.Drawing.Point(173, 184);
            this.cancel.Name = "cancel";
            this.cancel.Size = new System.Drawing.Size(74, 35);
            this.cancel.TabIndex = 3;
            this.cancel.Text = "Cancel";
            this.cancel.UseVisualStyleBackColor = true;
            // 
            // addressName
            // 
            this.addressName.AutoSize = true;
            this.addressName.Location = new System.Drawing.Point(16, 32);
            this.addressName.Name = "addressName";
            this.addressName.Size = new System.Drawing.Size(73, 13);
            this.addressName.TabIndex = 4;
            this.addressName.Text = "Email Address";
            // 
            // passwordName
            // 
            this.passwordName.AutoSize = true;
            this.passwordName.Location = new System.Drawing.Point(16, 113);
            this.passwordName.Name = "passwordName";
            this.passwordName.Size = new System.Drawing.Size(53, 13);
            this.passwordName.TabIndex = 5;
            this.passwordName.Text = "Password";
            // 
            // Username_Password
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(298, 240);
            this.Controls.Add(this.passwordName);
            this.Controls.Add(this.addressName);
            this.Controls.Add(this.cancel);
            this.Controls.Add(this.logIn);
            this.Controls.Add(this.password);
            this.Controls.Add(this.userName);
            this.Name = "Username_Password";
            this.Text = "Email Log In";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox userName;
        private System.Windows.Forms.TextBox password;
        private System.Windows.Forms.Button logIn;
        private System.Windows.Forms.Button cancel;
        private System.Windows.Forms.Label addressName;
        private System.Windows.Forms.Label passwordName;
    }
}